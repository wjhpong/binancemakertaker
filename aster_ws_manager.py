"""Aster DEX WebSocket 管理器 —— 实时行情推送 + 用户数据流。

架构与 ws_manager.py (Binance) 完全一致，差异：
  - 域名: sstream.asterdex.com / fstream.asterdex.com
  - 现货 listenKey: POST /api/v1/listenKey (Binance 用 /api/v3/userDataStream)
  - 现货 executionReport 格式与 Binance 兼容
  - 复用 ws_manager.PriceCache（交易所无关）
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
from typing import Callable, Optional

import requests
import websockets.sync.client as ws_sync

from ws_manager import PriceCache  # 复用通用的价格缓存

logger = logging.getLogger(__name__)


class AsterWSManager:
    """管理 Aster 现货深度 + 合约 bookTicker WebSocket 连接 + 用户数据流。"""

    # ── Aster endpoint ──
    _SPOT_DEPTH_WS = "wss://sstream.asterdex.com/ws/{symbol}@depth5@100ms"
    _FUT_WS = "wss://fstream.asterdex.com/ws/{symbol}@bookTicker"
    _SPOT_REST = "https://sapi.asterdex.com"
    _SPOT_WS_BASE = "wss://sstream.asterdex.com/ws/"

    def __init__(
        self,
        symbol: str,
        api_key: str = "",
        on_order_update: Callable[[dict], None] | None = None,
    ) -> None:
        self.symbol = symbol.lower()
        self.price_cache = PriceCache()
        self._api_key = api_key
        self._on_order_update = on_order_update
        self.fill_queue: queue.Queue[dict] = queue.Queue()
        self._running = False
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        self._running = True

        t_spot = threading.Thread(
            target=self._run_ws,
            args=(self._build_spot_depth_url(), self._handle_spot_depth),
            name="aster-ws-spot-depth",
            daemon=True,
        )
        t_fut = threading.Thread(
            target=self._run_ws,
            args=(self._build_futures_url(), self._handle_futures_book),
            name="aster-ws-fut-book",
            daemon=True,
        )

        t_spot.start()
        t_fut.start()
        self._threads = [t_spot, t_fut]

        # 启动用户数据流（需要 api_key）
        if self._api_key:
            t_user = threading.Thread(
                target=self._run_user_data_stream,
                name="aster-ws-user-data",
                daemon=True,
            )
            t_user.start()
            self._threads.append(t_user)

        logger.info("Aster WebSocket 已启动: symbol=%s (spot=depth5, fut=bookTicker), user_stream=%s",
                     self.symbol, bool(self._api_key))

    def stop(self) -> None:
        self._running = False
        logger.info("Aster WebSocket 正在关闭...")

    # ── URL 构建 ──

    def _build_spot_depth_url(self) -> str:
        return self._SPOT_DEPTH_WS.format(symbol=self.symbol)

    def _build_futures_url(self) -> str:
        return self._FUT_WS.format(symbol=self.symbol)

    # ── WS 循环（自动重连）──

    def _run_ws(self, url: str, handler) -> None:
        reconnect_delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                logger.info("Aster WS 连接: %s", url)
                with ws_sync.connect(url, close_timeout=5) as conn:
                    reconnect_delay = 1.0
                    while self._running:
                        try:
                            msg = conn.recv(timeout=5)
                        except TimeoutError:
                            continue
                        data = json.loads(msg)
                        handler(data)
            except Exception:
                if not self._running:
                    break
                logger.exception("Aster WS 断线: %s, %.1f秒后重连", url, reconnect_delay)
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_delay)

    # ── 消息处理 ──

    def _handle_spot_depth(self, data: dict) -> None:
        """解析 Aster 现货深度推送。

        Aster 用 depthUpdate 格式: {"e":"depthUpdate", "b":[...], "a":[...]}
        也兼容 {"bids":[...], "asks":[...]} 格式。
        """
        try:
            raw_bids = data.get("b") or data.get("bids")
            raw_asks = data.get("a") or data.get("asks")
            if raw_bids is None or raw_asks is None:
                logger.warning("Aster 现货深度数据缺少 b/a 字段: %s", data)
                return
            bids = [(float(p), float(q)) for p, q in raw_bids]
            asks = [(float(p), float(q)) for p, q in raw_asks]
            self.price_cache.update_spot_depth(bids, asks)
        except (ValueError, TypeError):
            logger.warning("Aster 现货深度数据解析失败: %s", data)

    def _handle_futures_book(self, data: dict) -> None:
        """解析合约 bookTicker: {"b":"bid","a":"ask",...}"""
        try:
            self.price_cache.update_futures(
                bid=float(data["b"]),
                ask=float(data["a"]),
            )
        except (KeyError, ValueError):
            logger.warning("Aster 合约 bookTicker 数据异常: %s", data)

    # ── 用户数据流 ──

    def _create_listen_key(self) -> str:
        """创建 listenKey 用于用户数据流。"""
        url = f"{self._SPOT_REST}/api/v1/listenKey"
        headers = {"X-MBX-APIKEY": self._api_key}
        resp = requests.post(url, headers=headers, timeout=10)
        resp.raise_for_status()
        key = resp.json()["listenKey"]
        logger.info("Aster listenKey 已创建")
        return key

    def _keepalive_listen_key(self, listen_key: str) -> None:
        """延长 listenKey 有效期。"""
        url = f"{self._SPOT_REST}/api/v1/listenKey"
        headers = {"X-MBX-APIKEY": self._api_key}
        try:
            resp = requests.put(url, headers=headers, params={"listenKey": listen_key}, timeout=10)
            resp.raise_for_status()
            logger.debug("Aster listenKey keepalive 成功")
        except Exception:
            logger.exception("Aster listenKey keepalive 失败")

    def _run_user_data_stream(self) -> None:
        """运行用户数据流 WebSocket，接收订单状态更新。"""
        reconnect_delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                listen_key = self._create_listen_key()
                ws_url = f"{self._SPOT_WS_BASE}{listen_key}"
                logger.info("Aster 用户数据流连接: %s", ws_url[:60] + "...")

                last_keepalive = time.time()

                with ws_sync.connect(ws_url, close_timeout=5) as conn:
                    reconnect_delay = 1.0
                    while self._running:
                        # keepalive 每 25 分钟（listenKey 60分钟过期）
                        if time.time() - last_keepalive > 25 * 60:
                            self._keepalive_listen_key(listen_key)
                            last_keepalive = time.time()

                        try:
                            msg = conn.recv(timeout=5)
                        except TimeoutError:
                            continue

                        data = json.loads(msg)
                        self._handle_user_data(data)

            except Exception:
                if not self._running:
                    break
                logger.exception("Aster 用户数据流断线, %.1f秒后重连", reconnect_delay)
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_delay)

    def _handle_user_data(self, data: dict) -> None:
        """处理用户数据流消息。Aster 的 executionReport 格式与 Binance 完全兼容。"""
        event_type = data.get("e")

        if event_type == "executionReport":
            # 现货订单执行报告 —— 格式与 Binance 相同
            order_status = data.get("X")  # NEW, PARTIALLY_FILLED, FILLED, CANCELED
            exec_type = data.get("x")     # NEW, TRADE, CANCELED
            symbol = data.get("s")
            order_id = str(data.get("i", ""))
            side = data.get("S")          # BUY / SELL
            filled_qty = float(data.get("z", 0))       # 累计成交量
            last_filled_qty = float(data.get("l", 0))   # 本次成交量
            last_filled_price = float(data.get("L", 0)) # 本次成交价

            logger.info(
                "[ASTER_USER_STREAM] %s | symbol=%s side=%s order_id=%s "
                "status=%s filled=%.8f last_qty=%.8f last_price=%.2f",
                exec_type, symbol, side, order_id,
                order_status, filled_qty, last_filled_qty, last_filled_price,
            )

            # 如果是买单成交（TRADE），推入 fill_queue 供主循环消费
            if exec_type == "TRADE" and side == "BUY":
                fill_event = {
                    "symbol": symbol,
                    "order_id": order_id,
                    "filled_qty": filled_qty,
                    "last_filled_qty": last_filled_qty,
                    "last_filled_price": last_filled_price,
                    "status": order_status,
                }
                self.fill_queue.put(fill_event)

            # 回调
            if self._on_order_update:
                self._on_order_update(data)

        elif event_type == "outboundAccountPosition":
            logger.debug("[ASTER_USER_STREAM] 账户持仓更新")
        elif event_type == "balanceUpdate":
            logger.debug("[ASTER_USER_STREAM] 余额更新")
