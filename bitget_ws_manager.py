"""Bitget WebSocket 管理器 —— 现货深度 + 用户订单流。

架构与 WSManager / GateWSManager 对齐：
  - PriceCache: 线程安全价格缓存（复用 ws_manager.py 中的 PriceCache）
  - fill_queue: 买单成交事件队列（供 FillHandler 消费）
  - 两个 daemon 线程: spot depth (public) + user orders (private)

Bitget WS v2 文档:
  https://www.bitget.com/api-doc/common/websocket-intro

公共频道: wss://ws.bitget.com/v2/ws/public
私有频道: wss://ws.bitget.com/v2/ws/private
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import queue
import threading
import time
from typing import Callable, Optional

import websockets.sync.client as ws_sync

from ws_manager import PriceCache  # 复用现有 PriceCache

logger = logging.getLogger(__name__)


class BitgetWSManager:
    """Bitget 现货 WebSocket 管理器。"""

    _WS_PUBLIC = "wss://ws.bitget.com/v2/ws/public"
    _WS_PRIVATE = "wss://ws.bitget.com/v2/ws/private"

    def __init__(
        self,
        symbol: str,
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
        on_order_update: Callable[[dict], None] | None = None,
    ) -> None:
        """
        Args:
            symbol: 交易对，如 "ASTERUSDT"（Bitget v2 格式与 Binance 一致）
            api_key: Bitget API key（用于用户数据流）
            api_secret: Bitget API secret
            passphrase: Bitget API passphrase
        """
        self._symbol = symbol.upper()
        self._api_key = api_key
        self._api_secret = api_secret
        self._passphrase = passphrase
        self._on_order_update = on_order_update

        self.price_cache = PriceCache()
        self.fill_queue: queue.Queue[dict] = queue.Queue()
        self._running = False
        self._threads: list[threading.Thread] = []

    # ── 启动 / 停止 ──────────────────────────────────────────────

    def start(self) -> None:
        self._running = True

        # 线程 1: 现货深度（公共频道）
        t_depth = threading.Thread(
            target=self._run_depth_stream,
            name="bitget-ws-depth",
            daemon=True,
        )
        t_depth.start()
        self._threads = [t_depth]

        # 线程 2: 用户订单流（私有频道，需要 api_key）
        if self._api_key and self._api_secret:
            t_user = threading.Thread(
                target=self._run_user_stream,
                name="bitget-ws-user",
                daemon=True,
            )
            t_user.start()
            self._threads.append(t_user)

        logger.info(
            "[BITGET WS] 已启动: symbol=%s, depth=✓, user_stream=%s",
            self._symbol, bool(self._api_key),
        )

    def stop(self) -> None:
        self._running = False
        logger.info("[BITGET WS] 正在关闭...")

    # ── WS 签名（私有频道登录）───────────────────────────────────

    def _ws_login_msg(self) -> dict:
        """生成 Bitget WS 登录消息。

        签名字符串: timestamp + "GET" + "/user/verify"
        HMAC-SHA256 → Base64
        """
        timestamp = str(int(time.time()))
        prehash = timestamp + "GET" + "/user/verify"
        mac = hmac.new(
            self._api_secret.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        )
        sign = base64.b64encode(mac.digest()).decode("utf-8")
        return {
            "op": "login",
            "args": [{
                "apiKey": self._api_key,
                "passphrase": self._passphrase,
                "timestamp": timestamp,
                "sign": sign,
            }],
        }

    # ── 深度流 ───────────────────────────────────────────────────

    def _run_depth_stream(self) -> None:
        """订阅 books5 频道（公共），更新 PriceCache。"""
        reconnect_delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                logger.info("[BITGET WS] 连接深度流: %s", self._WS_PUBLIC)
                with ws_sync.connect(self._WS_PUBLIC, close_timeout=5) as conn:
                    reconnect_delay = 1.0

                    # 订阅 books5（top 5 snapshot，无需增量管理）
                    sub_msg = {
                        "op": "subscribe",
                        "args": [{
                            "instType": "SPOT",
                            "channel": "books5",
                            "instId": self._symbol,
                        }],
                    }
                    conn.send(json.dumps(sub_msg))
                    logger.info("[BITGET WS] 已订阅深度: %s", self._symbol)

                    last_ping = time.time()
                    while self._running:
                        try:
                            msg = conn.recv(timeout=5)
                        except TimeoutError:
                            # 发送 ping 保持连接
                            if time.time() - last_ping > 25:
                                try:
                                    conn.send("ping")
                                    last_ping = time.time()
                                except Exception:
                                    break
                            continue

                        if msg == "pong":
                            continue

                        try:
                            data = json.loads(msg)
                        except json.JSONDecodeError:
                            continue

                        self._handle_depth(data)

            except Exception:
                if not self._running:
                    break
                logger.exception(
                    "[BITGET WS] 深度流断线, %.1f秒后重连", reconnect_delay
                )
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_delay)

    def _handle_depth(self, data: dict) -> None:
        """处理 books5 推送。

        格式: {"action": "snapshot", "arg": {"instType":"SPOT","channel":"books5","instId":"BTCUSDT"},
               "data": [{"asks": [["price","qty"],...], "bids": [["price","qty"],...], "ts": "..."}]}
        """
        arg = data.get("arg", {})
        channel = arg.get("channel", "")

        if channel == "books5":
            items = data.get("data", [])
            if not items:
                return
            book = items[0]
            try:
                bids_raw = book.get("bids", [])
                asks_raw = book.get("asks", [])
                bids = [(float(p), float(q)) for p, q in bids_raw]
                asks = [(float(p), float(q)) for p, q in asks_raw]
                if bids or asks:
                    self.price_cache.update_spot_depth(bids, asks)
            except (ValueError, TypeError):
                logger.warning("[BITGET WS] 深度数据异常: %s", data)

        elif data.get("event") == "subscribe":
            code = data.get("code", 0)
            if str(code) != "0":
                logger.error("[BITGET WS] 订阅失败: %s", data)
            else:
                logger.debug("[BITGET WS] 订阅确认: %s", data)

    # ── 用户订单流 ───────────────────────────────────────────────

    def _run_user_stream(self) -> None:
        """连接私有 WS，登录后订阅 orders 频道。"""
        reconnect_delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                logger.info("[BITGET WS] 连接用户订单流: %s", self._WS_PRIVATE)
                with ws_sync.connect(self._WS_PRIVATE, close_timeout=5) as conn:
                    reconnect_delay = 1.0

                    # 步骤 1: 登录
                    login_msg = self._ws_login_msg()
                    conn.send(json.dumps(login_msg))
                    logger.info("[BITGET WS] 发送登录请求...")

                    # 等待登录响应
                    login_resp_raw = conn.recv(timeout=10)
                    if login_resp_raw == "pong":
                        login_resp_raw = conn.recv(timeout=10)
                    try:
                        login_resp = json.loads(login_resp_raw)
                    except json.JSONDecodeError:
                        logger.error("[BITGET WS] 登录响应解析失败: %s", login_resp_raw)
                        time.sleep(reconnect_delay)
                        continue

                    if login_resp.get("event") != "login" or str(login_resp.get("code", "1")) != "0":
                        logger.error("[BITGET WS] 登录失败: %s", login_resp)
                        time.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, max_delay)
                        continue

                    logger.info("[BITGET WS] 登录成功")

                    # 步骤 2: 订阅 orders 频道
                    sub_msg = {
                        "op": "subscribe",
                        "args": [{
                            "instType": "SPOT",
                            "channel": "orders",
                            "instId": self._symbol,
                        }],
                    }
                    conn.send(json.dumps(sub_msg))
                    logger.info("[BITGET WS] 已订阅用户订单流: %s", self._symbol)

                    last_ping = time.time()
                    while self._running:
                        try:
                            msg = conn.recv(timeout=5)
                        except TimeoutError:
                            # ping 保活
                            if time.time() - last_ping > 25:
                                try:
                                    conn.send("ping")
                                    last_ping = time.time()
                                except Exception:
                                    break
                            continue

                        if msg == "pong":
                            continue

                        try:
                            data = json.loads(msg)
                        except json.JSONDecodeError:
                            continue

                        self._handle_user_data(data)

            except Exception:
                if not self._running:
                    break
                logger.exception(
                    "[BITGET WS] 用户订单流断线, %.1f秒后重连", reconnect_delay
                )
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_delay)

    def _handle_user_data(self, data: dict) -> None:
        """处理 orders 频道推送。

        格式: {"action": "snapshot", "arg": {"instType":"SPOT","channel":"orders","instId":"..."},
               "data": [{
                   "instId": "ASTERUSDT",
                   "orderId": "12345",
                   "side": "buy",
                   "size": "100",
                   "accBaseVolume": "50",
                   "fillPrice": "0.123",
                   "status": "filled",
                   ...
               }]}
        """
        arg = data.get("arg", {})
        channel = arg.get("channel", "")

        if channel == "orders":
            items = data.get("data", [])
            if not isinstance(items, list):
                items = [items]

            for order in items:
                try:
                    side = order.get("side", "")
                    status = order.get("status", "")
                    order_id = str(order.get("orderId", ""))
                    # accBaseVolume: 累计成交基础币数量
                    acc_base_volume = float(order.get("accBaseVolume", 0))
                    fill_price = float(order.get("fillPrice", 0))

                    logger.info(
                        "[BITGET USER] side=%s id=%s status=%s "
                        "accBaseVol=%.8f fillPrice=%.8f",
                        side, order_id, status, acc_base_volume, fill_price,
                    )

                    # 买单成交 → 推入 fill_queue
                    if side == "buy" and acc_base_volume > 0:
                        fill_event = {
                            "symbol": self._symbol,
                            "order_id": order_id,
                            "filled_qty": acc_base_volume,
                            "last_filled_qty": acc_base_volume,
                            "last_filled_price": fill_price,
                            "status": self._map_status(status),
                        }
                        self.fill_queue.put(fill_event)

                    # 回调
                    if self._on_order_update:
                        self._on_order_update(order)

                except (ValueError, TypeError, KeyError):
                    logger.warning("[BITGET WS] 订单数据异常: %s", order)

        elif data.get("event") == "subscribe":
            code = data.get("code", 0)
            if str(code) != "0":
                logger.error("[BITGET WS] 订阅失败: %s", data)
            else:
                logger.debug("[BITGET WS] 订阅确认: %s", data)
        elif data.get("event") == "login":
            pass  # 登录响应，已在 _run_user_stream 中处理

    @staticmethod
    def _map_status(bitget_status: str) -> str:
        """Bitget 订单状态 → Binance 风格状态。"""
        mapping = {
            "init": "NEW",
            "new": "NEW",
            "partially_filled": "PARTIALLY_FILLED",
            "filled": "FILLED",
            "cancelled": "CANCELED",
            "canceled": "CANCELED",
        }
        return mapping.get(bitget_status, bitget_status.upper())
