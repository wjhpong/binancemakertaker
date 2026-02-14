"""Gate.io v4 现货适配器 —— 用于跨所套利模式的现货端。

签名方式: HMAC-SHA512
API 文档: https://www.gate.com/docs/developers/apiv4/en/

仅实现现货方法；合约方法 raise NotImplementedError，
在 CrossExchangeAdapter 中永远不会被调用。
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
import time
from collections import deque
from typing import Optional
from urllib.parse import urlencode

import requests

from arbitrage_bot import ExchangeAdapter

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.gateio.ws/api/v4"

# Gate.io error codes
_ERR_ORDER_NOT_FOUND = "ORDER_NOT_FOUND"


class RateLimiter:
    """简易滑动窗口限速器。"""

    def __init__(self, max_weight: int = 900, window_sec: int = 60) -> None:
        self._max = max_weight
        self._window = window_sec
        self._requests: deque[tuple[float, int]] = deque()
        self._lock = threading.Lock()

    def record(self, weight: int = 1) -> None:
        with self._lock:
            now = time.time()
            self._requests.append((now, weight))
            self._cleanup(now)

    def _cleanup(self, now: float) -> None:
        cutoff = now - self._window
        while self._requests and self._requests[0][0] < cutoff:
            self._requests.popleft()

    def current_weight(self) -> int:
        with self._lock:
            self._cleanup(time.time())
            return sum(w for _, w in self._requests)

    def wait_if_needed(self, weight: int = 1) -> None:
        while True:
            with self._lock:
                now = time.time()
                self._cleanup(now)
                total = sum(w for _, w in self._requests)
                if total + weight <= self._max:
                    self._requests.append((now, weight))
                    return
            time.sleep(0.2)


class GateAdapter(ExchangeAdapter):
    """Gate.io v4 现货 API 适配器。"""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        price_cache=None,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self.price_cache = price_cache
        self._session = requests.Session()
        self._limiter = RateLimiter(max_weight=900, window_sec=60)
        self._last_hedge_avg_price: float | None = None

    # ── 符号转换 ─────────────────────────────────────────────────

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

    @staticmethod
    def _from_gate_pair(gate_pair: str) -> str:
        """ASTER_USDT → ASTERUSDT"""
        return gate_pair.replace("_", "")

    # ── 签名 ─────────────────────────────────────────────────────

    def _sign(self, method: str, url_path: str, query_string: str = "", body: str = "") -> dict:
        """生成 Gate.io v4 请求头。

        签名字符串 = method + "\n" + url_path + "\n" + query_string + "\n" + SHA512(body) + "\n" + timestamp
        """
        t = str(int(time.time()))
        hashed_body = hashlib.sha512(body.encode("utf-8")).hexdigest()
        sign_str = f"{method}\n{url_path}\n{query_string}\n{hashed_body}\n{t}"
        signature = hmac.new(
            self._api_secret.encode("utf-8"),
            sign_str.encode("utf-8"),
            hashlib.sha512,
        ).hexdigest()
        return {
            "KEY": self._api_key,
            "SIGN": signature,
            "Timestamp": t,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    # ── HTTP 请求 ────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        body: dict | None = None,
        signed: bool = True,
        weight: int = 1,
    ) -> dict | list:
        """发送 Gate.io API v4 请求。"""
        self._limiter.wait_if_needed(weight)

        url_path = f"/api/v4{path}"
        query_string = urlencode(params) if params else ""
        body_str = json.dumps(body) if body else ""

        url = f"{_BASE_URL}{path}"
        if query_string:
            url += f"?{query_string}"

        if signed:
            headers = self._sign(method, url_path, query_string, body_str)
        else:
            headers = {"Content-Type": "application/json", "Accept": "application/json"}

        resp = self._session.request(
            method,
            url,
            headers=headers,
            data=body_str if body else None,
            timeout=10,
        )

        if resp.status_code >= 400:
            try:
                err = resp.json()
            except Exception:
                err = resp.text
            label = err.get("label", "") if isinstance(err, dict) else ""
            msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
            raise RuntimeError(
                f"Gate.io ({resp.status_code}, {label}, '{msg}')"
            )

        return resp.json()

    # ── 现货方法实现 ─────────────────────────────────────────────

    def get_spot_depth(self, symbol_spot: str, levels: int = 5) -> list[tuple[float, float]]:
        """返回现货买盘深度（bid），优先 WS 缓存。"""
        if self.price_cache is not None:
            bids = self.price_cache.get_spot_bids(n=levels)
            if bids and not self.price_cache.is_spot_depth_stale():
                return bids

        pair = self._to_gate_pair(symbol_spot)
        data = self._request("GET", "/spot/order_book", {"currency_pair": pair, "limit": str(levels)}, signed=False, weight=2)
        bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
        return bids

    def get_spot_asks(self, symbol_spot: str, levels: int = 5) -> list[tuple[float, float]]:
        """返回现货卖盘深度（ask），优先 WS 缓存。"""
        if self.price_cache is not None:
            asks = self.price_cache.get_spot_asks(n=levels)
            if asks and not self.price_cache.is_spot_depth_stale():
                return asks

        pair = self._to_gate_pair(symbol_spot)
        data = self._request("GET", "/spot/order_book", {"currency_pair": pair, "limit": str(levels)}, signed=False, weight=2)
        asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
        return asks

    def get_spot_open_bid_order(self, symbol_spot: str) -> Optional[dict]:
        """查询当前现货买单。"""
        pair = self._to_gate_pair(symbol_spot)
        orders = self._request("GET", "/spot/open_orders", {"currency_pair": pair}, weight=2)
        # Gate 返回 [{"currency_pair": ..., "orders": [...]}]
        for entry in orders:
            for order in entry.get("orders", []):
                if order.get("side") == "buy":
                    return {
                        "orderId": str(order["id"]),
                        "price": float(order["price"]),
                        "origQty": float(order["amount"]),
                        "executedQty": float(order.get("filled_amount", 0)),
                    }
        return None

    def place_spot_limit_buy(self, symbol_spot: str, price: float, qty: float) -> str:
        """下现货限价买单。"""
        pair = self._to_gate_pair(symbol_spot)
        body = {
            "currency_pair": pair,
            "side": "buy",
            "type": "limit",
            "price": str(price),
            "amount": str(qty),
            "time_in_force": "gtc",
        }
        resp = self._request("POST", "/spot/orders", body=body, weight=2)
        order_id = str(resp["id"])
        logger.info(
            "[GATE] 现货限价买单 %s qty=%.6f @ %.8f → order_id=%s",
            pair, qty, price, order_id,
        )
        return order_id

    def place_spot_limit_sell(self, symbol_spot: str, price: float, qty: float) -> str:
        """下现货限价卖单。"""
        pair = self._to_gate_pair(symbol_spot)
        body = {
            "currency_pair": pair,
            "side": "sell",
            "type": "limit",
            "price": str(price),
            "amount": str(qty),
            "time_in_force": "gtc",
        }
        resp = self._request("POST", "/spot/orders", body=body, weight=2)
        order_id = str(resp["id"])
        logger.info(
            "[GATE] 现货限价卖单 %s qty=%.6f @ %.8f → order_id=%s",
            pair, qty, price, order_id,
        )
        return order_id

    def cancel_order(self, symbol: str, order_id: str) -> None:
        """撤销现货订单。"""
        pair = self._to_gate_pair(symbol)
        try:
            self._request(
                "DELETE",
                f"/spot/orders/{order_id}",
                params={"currency_pair": pair},
                weight=1,
            )
            logger.info("[GATE] 已撤单 %s order_id=%s", pair, order_id)
        except RuntimeError as e:
            err_str = str(e)
            if "ORDER_NOT_FOUND" in err_str or "order not found" in err_str.lower():
                logger.warning("[GATE] 撤单时订单不存在 (已成交或已撤): %s", order_id)
            else:
                raise

    def get_order_filled_qty(self, symbol: str, order_id: str) -> float:
        """查询订单累计成交量。返回 -1 表示订单不存在。"""
        pair = self._to_gate_pair(symbol)
        try:
            resp = self._request(
                "GET",
                f"/spot/orders/{order_id}",
                params={"currency_pair": pair},
                weight=1,
            )
            return float(resp.get("filled_amount", 0))
        except RuntimeError as e:
            if "ORDER_NOT_FOUND" in str(e) or "order not found" in str(e).lower():
                return -1.0
            raise

    def get_spot_balance(self, asset: str) -> float:
        """查询现货账户指定资产余额。"""
        accounts = self._request("GET", "/spot/accounts", weight=1)
        for acct in accounts:
            if acct.get("currency", "").upper() == asset.upper():
                return float(acct.get("available", 0))
        return 0.0

    def get_earn_balance(self, asset: str) -> float:
        """Gate 理财余额暂不支持，返回 0。"""
        return 0.0

    # ── Preflight ────────────────────────────────────────────────

    def preflight_check(self, symbol_spot: str, symbol_fut: str) -> dict:
        """查询交易对信息，返回 tick_size 和 lot_size。"""
        pair = self._to_gate_pair(symbol_spot)
        try:
            data = self._request("GET", f"/spot/currency_pairs/{pair}", signed=False, weight=1)
            # data: {"id": "ASTER_USDT", "base": "ASTER", "quote": "USDT",
            #         "fee": "0.2", "min_quote_amount": "1",
            #         "amount_precision": 2, "precision": 6, ...}
            amount_precision = int(data.get("amount_precision", 2))
            price_precision = int(data.get("precision", 6))
            spot_lot_size = 10 ** (-amount_precision)   # e.g., precision=2 → 0.01
            spot_tick_size = 10 ** (-price_precision)    # e.g., precision=6 → 0.000001

            logger.info(
                "[GATE] preflight %s: tick=%.8f lot=%.8f min_quote=%s",
                pair, spot_tick_size, spot_lot_size, data.get("min_quote_amount", "?"),
            )
            return {
                "spot_tick_size": spot_tick_size,
                "spot_lot_size": spot_lot_size,
                "fut_tick_size": None,
                "fut_lot_size": None,
            }
        except Exception:
            logger.exception("[GATE] preflight_check 失败: %s", pair)
            return {}

    # ── 合约方法（跨所模式下不会被调用）────────────────────────

    def get_futures_best_bid(self, symbol_fut: str) -> float:
        raise NotImplementedError("GateAdapter 不支持合约操作，请使用 CrossExchangeAdapter")

    def get_futures_best_ask(self, symbol_fut: str) -> float:
        raise NotImplementedError("GateAdapter 不支持合约操作，请使用 CrossExchangeAdapter")

    def place_futures_market_sell(self, symbol_fut: str, qty: float) -> str:
        raise NotImplementedError("GateAdapter 不支持合约操作，请使用 CrossExchangeAdapter")

    def place_futures_market_buy(self, symbol_fut: str, qty: float) -> str:
        raise NotImplementedError("GateAdapter 不支持合约操作，请使用 CrossExchangeAdapter")

    def get_futures_position(self, symbol_fut: str) -> float:
        raise NotImplementedError("GateAdapter 不支持合约操作，请使用 CrossExchangeAdapter")
