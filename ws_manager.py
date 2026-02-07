"""WebSocket 管理器 —— 实时行情推送 + 用户数据流。

架构：
  - 主线程：同步运行 bot 主循环
  - daemon 线程：运行 WS 连接，推送价格到 PriceCache
  - PriceCache：线程安全的价格缓存，供 BinanceAdapter 读取
  - 用户数据流：通过 listenKey 接收订单成交推送
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

logger = logging.getLogger(__name__)


class PriceCache:
    """线程安全的盘口价格缓存，支持多档深度。

    spot_bids / spot_asks: [(price, qty), ...] 按价格从优到劣排列
    fut_bid / fut_ask: 合约最优买一卖一（bookTicker 即可）
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # 现货多档深度
        self._spot_bids: list[tuple[float, float]] = []  # [(price, qty), ...]
        self._spot_asks: list[tuple[float, float]] = []
        self._spot_depth_ts: float = 0.0
        # 合约买一卖一
        self._fut_bid: Optional[float] = None
        self._fut_ask: Optional[float] = None
        self._fut_ts: float = 0.0

    def update_spot_depth(self, bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> None:
        """更新现货多档深度。bids: 买盘（价格从高到低），asks: 卖盘（价格从低到高）。"""
        with self._lock:
            self._spot_bids = bids
            self._spot_asks = asks
            self._spot_depth_ts = time.time()

    def update_futures(self, bid: float, ask: float) -> None:
        with self._lock:
            self._fut_bid = bid
            self._fut_ask = ask
            self._fut_ts = time.time()

    # ── 读取 ──

    def get_futures_bid(self) -> Optional[float]:
        with self._lock:
            return self._fut_bid

    def get_spot_bids(self, n: int = 20) -> list[tuple[float, float]]:
        """返回现货买盘前 n 档 [(price, qty), ...]，价格从高到低。"""
        with self._lock:
            return self._spot_bids[:n]

    def get_spot_best_bid(self) -> Optional[float]:
        with self._lock:
            return self._spot_bids[0][0] if self._spot_bids else None

    def is_stale(self, max_age_sec: float = 5.0) -> bool:
        with self._lock:
            if self._fut_ts == 0.0 or self._spot_depth_ts == 0.0:
                return True
            now = time.time()
            return (now - self._fut_ts) > max_age_sec or (now - self._spot_depth_ts) > max_age_sec

    def is_spot_depth_stale(self, max_age_sec: float = 5.0) -> bool:
        with self._lock:
            if self._spot_depth_ts == 0.0:
                return True
            return (time.time() - self._spot_depth_ts) > max_age_sec


class WSManager:
    """管理现货深度 + 合约 bookTicker WebSocket 连接 + 用户数据流。"""

    # ── 公网 endpoint ──
    _SPOT_DEPTH_WS = "wss://stream.binance.com:9443/ws/{symbol}@depth5@100ms"
    _FUT_WS = "wss://fstream.binance.com/ws/{symbol}@bookTicker"
    _SPOT_REST = "https://api.binance.com"
    _SPOT_WS_BASE = "wss://stream.binance.com:9443/ws/"

    # ── 测试网 endpoint ──
    _SPOT_DEPTH_WS_TESTNET = "wss://testnet.binance.vision/ws/{symbol}@depth5@100ms"
    _FUT_WS_TESTNET = "wss://stream.binancefuture.com/ws/{symbol}@bookTicker"
    _SPOT_REST_TESTNET = "https://testnet.binance.vision"
    _SPOT_WS_BASE_TESTNET = "wss://testnet.binance.vision/ws/"

    def __init__(
        self,
        symbol: str,
        testnet: bool = False,
        api_key: str = "",
        on_order_update: Callable[[dict], None] | None = None,
    ) -> None:
        self.symbol = symbol.lower()
        self.price_cache = PriceCache()
        self._testnet = testnet
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
            name="ws-spot-depth",
            daemon=True,
        )
        t_fut = threading.Thread(
            target=self._run_ws,
            args=(self._build_futures_url(), self._handle_futures_book),
            name="ws-fut-book",
            daemon=True,
        )

        t_spot.start()
        t_fut.start()
        self._threads = [t_spot, t_fut]

        # 启动用户数据流（需要 api_key）
        if self._api_key:
            t_user = threading.Thread(
                target=self._run_user_data_stream,
                name="ws-user-data",
                daemon=True,
            )
            t_user.start()
            self._threads.append(t_user)

        logger.info("WebSocket 已启动: symbol=%s (spot=depth5, fut=bookTicker), testnet=%s, user_stream=%s",
                     self.symbol, self._testnet, bool(self._api_key))

    def stop(self) -> None:
        self._running = False
        logger.info("WebSocket 正在关闭...")

    # ── URL 构建 ──

    def _build_spot_depth_url(self) -> str:
        tpl = self._SPOT_DEPTH_WS_TESTNET if self._testnet else self._SPOT_DEPTH_WS
        return tpl.format(symbol=self.symbol)

    def _build_futures_url(self) -> str:
        tpl = self._FUT_WS_TESTNET if self._testnet else self._FUT_WS
        return tpl.format(symbol=self.symbol)

    # ── WS 循环（自动重连）──

    def _run_ws(self, url: str, handler) -> None:
        reconnect_delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                logger.info("WS 连接: %s", url)
                with ws_sync.connect(url, close_timeout=5) as conn:
                    reconnect_delay = 1.0  # 连接成功，重置退避
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
                logger.exception("WS 断线: %s, %.1f秒后重连", url, reconnect_delay)
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_delay)

    # ── 消息处理 ──

    def _handle_spot_depth(self, data: dict) -> None:
        """解析现货 depth5 推送: {"bids":[["price","qty"],...], "asks":[["price","qty"],...]}"""
        try:
            bids = [(float(p), float(q)) for p, q in data["bids"]]
            asks = [(float(p), float(q)) for p, q in data["asks"]]
            self.price_cache.update_spot_depth(bids, asks)
        except (KeyError, ValueError, TypeError):
            logger.warning("现货 depth5 数据异常: %s", data)

    def _handle_futures_book(self, data: dict) -> None:
        try:
            self.price_cache.update_futures(
                bid=float(data["b"]),
                ask=float(data["a"]),
            )
        except (KeyError, ValueError):
            logger.warning("合约 bookTicker 数据异常: %s", data)

    # ── 用户数据流 ──

    def _get_rest_base(self) -> str:
        return self._SPOT_REST_TESTNET if self._testnet else self._SPOT_REST

    def _get_ws_base(self) -> str:
        return self._SPOT_WS_BASE_TESTNET if self._testnet else self._SPOT_WS_BASE

    def _create_listen_key(self) -> str:
        """创建 listenKey 用于用户数据流。"""
        url = f"{self._get_rest_base()}/api/v3/userDataStream"
        headers = {"X-MBX-APIKEY": self._api_key}
        resp = requests.post(url, headers=headers, timeout=10)
        resp.raise_for_status()
        key = resp.json()["listenKey"]
        logger.info("listenKey 已创建")
        return key

    def _keepalive_listen_key(self, listen_key: str) -> None:
        """延长 listenKey 有效期（每30分钟调一次）。"""
        url = f"{self._get_rest_base()}/api/v3/userDataStream"
        headers = {"X-MBX-APIKEY": self._api_key}
        try:
            resp = requests.put(url, headers=headers, params={"listenKey": listen_key}, timeout=10)
            resp.raise_for_status()
            logger.debug("listenKey keepalive 成功")
        except Exception:
            logger.exception("listenKey keepalive 失败")

    def _run_user_data_stream(self) -> None:
        """运行用户数据流 WebSocket，接收订单状态更新。"""
        reconnect_delay = 1.0
        max_delay = 30.0

        while self._running:
            try:
                listen_key = self._create_listen_key()
                ws_url = f"{self._get_ws_base()}{listen_key}"
                logger.info("用户数据流连接: %s", ws_url[:60] + "...")

                # 启动 keepalive 定时器
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
                logger.exception("用户数据流断线, %.1f秒后重连", reconnect_delay)
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_delay)

    def _handle_user_data(self, data: dict) -> None:
        """处理用户数据流消息。"""
        event_type = data.get("e")

        if event_type == "executionReport":
            # 订单执行报告
            order_status = data.get("X")  # NEW, PARTIALLY_FILLED, FILLED, CANCELED ...
            exec_type = data.get("x")     # NEW, TRADE, CANCELED ...
            symbol = data.get("s")
            order_id = str(data.get("i", ""))
            side = data.get("S")          # BUY / SELL
            filled_qty = float(data.get("z", 0))       # 累计成交量
            last_filled_qty = float(data.get("l", 0))   # 本次成交量
            last_filled_price = float(data.get("L", 0)) # 本次成交价

            logger.info(
                "[USER_STREAM] %s | symbol=%s side=%s order_id=%s "
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
            logger.debug("[USER_STREAM] 账户持仓更新")
        elif event_type == "balanceUpdate":
            logger.debug("[USER_STREAM] 余额更新")
