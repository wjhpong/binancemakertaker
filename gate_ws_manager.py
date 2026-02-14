"""Gate.io WebSocket 管理器 —— 现货深度 + 用户订单流。

架构与 WSManager / AsterWSManager 对齐：
  - PriceCache: 线程安全价格缓存（复用 ws_manager.py 中的 PriceCache）
  - fill_queue: 买单成交事件队列（供 FillHandler 消费）
  - 两个 daemon 线程: spot depth + user orders

Gate.io WS v4 文档:
  https://www.gate.com/docs/developers/apiv4/ws/en/
"""

from __future__ import annotations

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


class GateWSManager:
    """Gate.io 现货 WebSocket 管理器。"""

    _WS_URL = "wss://api.gateio.ws/ws/v4/"

    def __init__(
        self,
        symbol: str,
        api_key: str = "",
        api_secret: str = "",
        on_order_update: Callable[[dict], None] | None = None,
    ) -> None:
        """
        Args:
            symbol: Binance 格式的交易对，如 "ASTERUSDT"
            api_key: Gate.io API key（用于用户数据流）
            api_secret: Gate.io API secret
        """
        # 内部使用 Gate 格式: ASTER_USDT
        self._binance_symbol = symbol.upper()
        self._gate_pair = self._to_gate_pair(symbol)
        self._api_key = api_key
        self._api_secret = api_secret
        self._on_order_update = on_order_update

        self.price_cache = PriceCache()
        self.fill_queue: queue.Queue[dict] = queue.Queue()
        self._running = False
        self._threads: list[threading.Thread] = []

    @staticmethod
    def _to_gate_pair(binance_symbol: str) -> str:
        """ASTERUSDT → ASTER_USDT"""
        s = binance_symbol.upper()
        if s.endswith("USDT"):
            return s[:-4] + "_USDT"
        if s.endswith("USDC"):
            return s[:-4] + "_USDC"
        if s.endswith("BTC"):
            return s[:-3] + "_BTC"
        return s

    # ── 启动 / 停止 ──────────────────────────────────────────────

    def start(self) -> None:
        self._running = True

        # 线程 1: 现货深度
        t_depth = threading.Thread(
            target=self._run_depth_stream,
            name="gate-ws-depth",
            daemon=True,
        )
        t_depth.start()
        self._threads = [t_depth]

        # 线程 2: 用户订单流（需要 api_key）
        if self._api_key and self._api_secret:
            t_user = threading.Thread(
                target=self._run_user_stream,
                name="gate-ws-user",
                daemon=True,
            )
            t_user.start()
            self._threads.append(t_user)

        logger.info(
            "[GATE WS] 已启动: pair=%s, depth=✓, user_stream=%s",
            self._gate_pair, bool(self._api_key),
        )

    def stop(self) -> None:
        self._running = False
        logger.info("[GATE WS] 正在关闭...")

    # ── WS 签名（用户数据流认证）────────────────────────────────

    def _ws_sign(self, channel: str, event: str, timestamp: int) -> dict:
        """生成 Gate.io WS 认证字段。

        签名字符串: channel={channel}&event={event}&time={timestamp}
        """
        s = f"channel={channel}&event={event}&time={timestamp}"
        signature = hmac.new(
            self._api_secret.encode("utf-8"),
            s.encode("utf-8"),
            hashlib.sha512,
        ).hexdigest()
        return {
            "method": "api_key",
            "KEY": self._api_key,
            "SIGN": signature,
        }

    # ── 深度流 ───────────────────────────────────────────────────

    def _run_depth_stream(self) -> None:
        """订阅 spot.order_book 频道，更新 PriceCache。"""
        reconnect_delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                logger.info("[GATE WS] 连接深度流: %s", self._WS_URL)
                with ws_sync.connect(self._WS_URL, close_timeout=5) as conn:
                    reconnect_delay = 1.0

                    # 订阅 order_book (公开频道，无需认证)
                    sub_msg = {
                        "time": int(time.time()),
                        "channel": "spot.order_book",
                        "event": "subscribe",
                        "payload": [self._gate_pair, "5", "100ms"],
                    }
                    conn.send(json.dumps(sub_msg))
                    logger.info("[GATE WS] 已订阅深度: %s", self._gate_pair)

                    while self._running:
                        try:
                            msg = conn.recv(timeout=5)
                        except TimeoutError:
                            # 发送 ping 保持连接
                            try:
                                ping_msg = {
                                    "time": int(time.time()),
                                    "channel": "spot.ping",
                                }
                                conn.send(json.dumps(ping_msg))
                            except Exception:
                                break
                            continue

                        data = json.loads(msg)
                        self._handle_depth(data)

            except Exception:
                if not self._running:
                    break
                logger.exception(
                    "[GATE WS] 深度流断线, %.1f秒后重连", reconnect_delay
                )
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_delay)

    def _handle_depth(self, data: dict) -> None:
        """处理 spot.order_book 推送。

        格式: {"channel": "spot.order_book", "event": "update",
               "result": {"bids": [["price", "qty"], ...], "asks": [...]}}
        """
        channel = data.get("channel", "")
        event = data.get("event", "")

        if channel == "spot.order_book" and event == "update":
            result = data.get("result", {})
            try:
                bids_raw = result.get("bids", [])
                asks_raw = result.get("asks", [])
                bids = [(float(p), float(q)) for p, q in bids_raw]
                asks = [(float(p), float(q)) for p, q in asks_raw]
                if bids or asks:
                    self.price_cache.update_spot_depth(bids, asks)
            except (ValueError, TypeError):
                logger.warning("[GATE WS] 深度数据异常: %s", data)
        elif event == "subscribe":
            logger.debug("[GATE WS] 订阅确认: %s", data)
        elif channel == "spot.pong":
            pass  # pong 回复，忽略

    # ── 用户订单流 ───────────────────────────────────────────────

    def _run_user_stream(self) -> None:
        """订阅 spot.orders 频道（需要认证），接收订单成交推送。"""
        reconnect_delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                logger.info("[GATE WS] 连接用户订单流: %s", self._WS_URL)
                with ws_sync.connect(self._WS_URL, close_timeout=5) as conn:
                    reconnect_delay = 1.0

                    # 订阅 spot.orders（私有频道，需要认证）
                    ts = int(time.time())
                    sub_msg = {
                        "time": ts,
                        "channel": "spot.orders",
                        "event": "subscribe",
                        "payload": [self._gate_pair],
                        "auth": self._ws_sign("spot.orders", "subscribe", ts),
                    }
                    conn.send(json.dumps(sub_msg))
                    logger.info("[GATE WS] 已订阅用户订单流: %s", self._gate_pair)

                    while self._running:
                        try:
                            msg = conn.recv(timeout=5)
                        except TimeoutError:
                            # ping 保活
                            try:
                                ping_msg = {
                                    "time": int(time.time()),
                                    "channel": "spot.ping",
                                }
                                conn.send(json.dumps(ping_msg))
                            except Exception:
                                break
                            continue

                        data = json.loads(msg)
                        self._handle_user_data(data)

            except Exception:
                if not self._running:
                    break
                logger.exception(
                    "[GATE WS] 用户订单流断线, %.1f秒后重连", reconnect_delay
                )
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_delay)

    def _handle_user_data(self, data: dict) -> None:
        """处理 spot.orders 推送。

        格式: {"channel": "spot.orders", "event": "update",
               "result": [{
                   "id": "12345",
                   "currency_pair": "ASTER_USDT",
                   "side": "buy",
                   "amount": "100",
                   "filled_amount": "50",
                   "fill_price": "0.123",
                   "status": "closed",
                   ...
               }]}
        """
        channel = data.get("channel", "")
        event = data.get("event", "")

        if channel == "spot.orders" and event == "update":
            results = data.get("result", [])
            if not isinstance(results, list):
                results = [results]

            for order in results:
                try:
                    side = order.get("side", "")
                    status = order.get("status", "")
                    order_id = str(order.get("id", ""))
                    filled_amount = float(order.get("filled_amount", 0))
                    fill_price = float(order.get("fill_price", 0))
                    amount = float(order.get("amount", 0))

                    logger.info(
                        "[GATE USER] side=%s id=%s status=%s "
                        "filled=%.8f fill_price=%.8f",
                        side, order_id, status, filled_amount, fill_price,
                    )

                    # 买单成交 → 推入 fill_queue
                    if side == "buy" and filled_amount > 0:
                        # 计算本次成交量（Gate 只给累计，需要差值）
                        # fill_handler 会基于累计量做去重，所以直接用累计值
                        fill_event = {
                            "symbol": self._binance_symbol,
                            "order_id": order_id,
                            "filled_qty": filled_amount,
                            "last_filled_qty": filled_amount,  # Gate 不提供增量，用累计
                            "last_filled_price": fill_price,
                            "status": self._map_status(status),
                        }
                        self.fill_queue.put(fill_event)

                    # 回调
                    if self._on_order_update:
                        self._on_order_update(order)

                except (ValueError, TypeError, KeyError):
                    logger.warning("[GATE WS] 订单数据异常: %s", order)

        elif event == "subscribe":
            err = data.get("error")
            if err:
                logger.error("[GATE WS] 订阅失败: %s", data)
            else:
                logger.debug("[GATE WS] 订阅确认: %s", data)

    @staticmethod
    def _map_status(gate_status: str) -> str:
        """Gate 订单状态 → Binance 风格状态。"""
        mapping = {
            "open": "NEW",
            "closed": "FILLED",
            "cancelled": "CANCELED",
            "expired": "EXPIRED",
        }
        return mapping.get(gate_status, gate_status.upper())
