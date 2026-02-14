"""Bitget v2 现货适配器 —— 用于跨所套利模式的现货端。

签名方式: HMAC-SHA256 + Base64
API 文档: https://www.bitget.com/api-doc/spot/

仅实现现货方法；合约方法 raise NotImplementedError，
在 CrossExchangeAdapter 中永远不会被调用。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import threading
import time
from collections import deque
from typing import Optional

import requests

from arbitrage_bot import ExchangeAdapter

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.bitget.com"


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


class BitgetAdapter(ExchangeAdapter):
    """Bitget v2 现货 API 适配器。"""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str = "",
        price_cache=None,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._passphrase = passphrase
        self.price_cache = price_cache
        self._session = requests.Session()
        self._limiter = RateLimiter(max_weight=900, window_sec=60)
        self._last_hedge_avg_price: float | None = None

    # ── 签名 ─────────────────────────────────────────────────────

    def _sign(self, timestamp: str, method: str, request_path: str, body: str = "") -> dict:
        """生成 Bitget v2 请求头。

        签名字符串 = timestamp + METHOD + requestPath + body
        HMAC-SHA256 → Base64
        """
        prehash = timestamp + method.upper() + request_path + body
        mac = hmac.new(
            self._api_secret.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        )
        signature = base64.b64encode(mac.digest()).decode("utf-8")
        return {
            "ACCESS-KEY": self._api_key,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type": "application/json",
            "locale": "en-US",
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
        """发送 Bitget API v2 请求。"""
        self._limiter.wait_if_needed(weight)

        timestamp = str(int(time.time() * 1000))
        query_string = ""
        if params:
            query_string = "&".join(f"{k}={v}" for k, v in params.items())
        body_str = json.dumps(body) if body else ""

        # 构造请求路径（含 query）
        request_path = path
        if query_string:
            request_path = f"{path}?{query_string}"

        url = f"{_BASE_URL}{request_path}"

        if signed:
            # 签名用 requestPath（含 query）+ body
            sign_body = body_str if method.upper() == "POST" else ""
            headers = self._sign(timestamp, method, request_path, sign_body)
        else:
            headers = {"Content-Type": "application/json"}

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
            code = err.get("code", "") if isinstance(err, dict) else ""
            msg = err.get("msg", str(err)) if isinstance(err, dict) else str(err)
            raise RuntimeError(
                f"Bitget ({resp.status_code}, code={code}, '{msg}')"
            )

        result = resp.json()
        # Bitget 统一信封: {"code": "00000", "msg": "success", "data": ...}
        code = result.get("code", "")
        if code != "00000":
            raise RuntimeError(
                f"Bitget (code={code}, '{result.get('msg', '')}')"
            )
        return result.get("data", result)

    # ── 现货方法实现 ─────────────────────────────────────────────

    def get_spot_depth(self, symbol_spot: str, levels: int = 5) -> list[tuple[float, float]]:
        """返回现货买盘深度（bid），优先 WS 缓存。"""
        if self.price_cache is not None:
            bids = self.price_cache.get_spot_bids(n=levels)
            if bids and not self.price_cache.is_spot_depth_stale():
                return bids

        symbol = symbol_spot.upper()
        data = self._request(
            "GET", "/api/v2/spot/market/orderbook",
            params={"symbol": symbol, "type": "step0", "limit": str(levels)},
            signed=False, weight=1,
        )
        bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
        return bids

    def get_spot_asks(self, symbol_spot: str, levels: int = 5) -> list[tuple[float, float]]:
        """返回现货卖盘深度（ask），优先 WS 缓存。"""
        if self.price_cache is not None:
            asks = self.price_cache.get_spot_asks(n=levels)
            if asks and not self.price_cache.is_spot_depth_stale():
                return asks

        symbol = symbol_spot.upper()
        data = self._request(
            "GET", "/api/v2/spot/market/orderbook",
            params={"symbol": symbol, "type": "step0", "limit": str(levels)},
            signed=False, weight=1,
        )
        asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
        return asks

    def get_spot_open_bid_order(self, symbol_spot: str) -> Optional[dict]:
        """查询当前现货买单。"""
        symbol = symbol_spot.upper()
        data = self._request(
            "GET", "/api/v2/spot/trade/unfilled-orders",
            params={"symbol": symbol},
            weight=2,
        )
        if not isinstance(data, list):
            data = data.get("orderList", []) if isinstance(data, dict) else []
        for order in data:
            if order.get("side") == "buy":
                return {
                    "orderId": str(order["orderId"]),
                    "price": float(order["price"]),
                    "origQty": float(order["size"]),
                    "executedQty": float(order.get("baseVolume", 0)),
                }
        return None

    def place_spot_limit_buy(self, symbol_spot: str, price: float, qty: float) -> str:
        """下现货限价买单。"""
        symbol = symbol_spot.upper()
        body = {
            "symbol": symbol,
            "side": "buy",
            "orderType": "limit",
            "force": "gtc",
            "price": str(price),
            "size": str(qty),
        }
        resp = self._request("POST", "/api/v2/spot/trade/place-order", body=body, weight=2)
        order_id = str(resp["orderId"])
        logger.info(
            "[BITGET] 现货限价买单 %s qty=%.6f @ %.8f → order_id=%s",
            symbol, qty, price, order_id,
        )
        return order_id

    def place_spot_limit_sell(self, symbol_spot: str, price: float, qty: float) -> str:
        """下现货限价卖单。"""
        symbol = symbol_spot.upper()
        body = {
            "symbol": symbol,
            "side": "sell",
            "orderType": "limit",
            "force": "gtc",
            "price": str(price),
            "size": str(qty),
        }
        resp = self._request("POST", "/api/v2/spot/trade/place-order", body=body, weight=2)
        order_id = str(resp["orderId"])
        logger.info(
            "[BITGET] 现货限价卖单 %s qty=%.6f @ %.8f → order_id=%s",
            symbol, qty, price, order_id,
        )
        return order_id

    def cancel_order(self, symbol: str, order_id: str) -> None:
        """撤销现货订单。"""
        body = {
            "symbol": symbol.upper(),
            "orderId": order_id,
        }
        try:
            self._request("POST", "/api/v2/spot/trade/cancel-order", body=body, weight=1)
            logger.info("[BITGET] 已撤单 %s order_id=%s", symbol, order_id)
        except RuntimeError as e:
            err_str = str(e)
            if "order not found" in err_str.lower() or "order does not exist" in err_str.lower():
                logger.warning("[BITGET] 撤单时订单不存在 (已成交或已撤): %s", order_id)
            else:
                raise

    def get_order_filled_qty(self, symbol: str, order_id: str) -> float:
        """查询订单累计成交量。返回 -1 表示订单不存在。"""
        try:
            data = self._request(
                "GET", "/api/v2/spot/trade/orderInfo",
                params={"orderId": order_id},
                weight=1,
            )
            # data 可能是列表或字典
            if isinstance(data, list):
                if not data:
                    return -1.0
                order = data[0]
            else:
                order = data
            return float(order.get("baseVolume", 0))
        except RuntimeError as e:
            if "order not found" in str(e).lower() or "does not exist" in str(e).lower():
                return -1.0
            raise

    def get_spot_balance(self, asset: str) -> float:
        """查询现货账户指定资产余额。"""
        data = self._request(
            "GET", "/api/v2/spot/account/assets",
            params={"coin": asset.upper()},
            weight=1,
        )
        if isinstance(data, list):
            for acct in data:
                if acct.get("coin", "").upper() == asset.upper():
                    return float(acct.get("available", 0))
        return 0.0

    def get_earn_balance(self, asset: str) -> float:
        """Bitget 理财余额暂不支持，返回 0。"""
        return 0.0

    # ── Preflight ────────────────────────────────────────────────

    def preflight_check(self, symbol_spot: str, symbol_fut: str) -> dict:
        """查询交易对信息，返回 tick_size 和 lot_size。"""
        symbol = symbol_spot.upper()
        try:
            data = self._request(
                "GET", "/api/v2/spot/public/symbols",
                params={"symbol": symbol},
                signed=False, weight=1,
            )
            if isinstance(data, list) and data:
                info = data[0]
            elif isinstance(data, dict):
                info = data
            else:
                logger.warning("[BITGET] preflight: 未找到交易对 %s", symbol)
                return {}

            price_precision = int(info.get("pricePrecision", 4))
            qty_precision = int(info.get("quantityPrecision", 4))
            spot_tick_size = 10 ** (-price_precision)
            spot_lot_size = 10 ** (-qty_precision)
            min_trade_amount = float(info.get("minTradeAmount", 0))

            logger.info(
                "[BITGET] preflight %s: tick=%.8f lot=%.8f min_amount=%.8f",
                symbol, spot_tick_size, spot_lot_size, min_trade_amount,
            )
            return {
                "spot_tick_size": spot_tick_size,
                "spot_lot_size": spot_lot_size,
                "fut_tick_size": None,
                "fut_lot_size": None,
            }
        except Exception:
            logger.exception("[BITGET] preflight_check 失败: %s", symbol)
            return {}

    # ── 合约方法（跨所模式下不会被调用）────────────────────────

    def get_futures_best_bid(self, symbol_fut: str) -> float:
        raise NotImplementedError("BitgetAdapter 不支持合约操作，请使用 CrossExchangeAdapter")

    def get_futures_best_ask(self, symbol_fut: str) -> float:
        raise NotImplementedError("BitgetAdapter 不支持合约操作，请使用 CrossExchangeAdapter")

    def place_futures_market_sell(self, symbol_fut: str, qty: float) -> str:
        raise NotImplementedError("BitgetAdapter 不支持合约操作，请使用 CrossExchangeAdapter")

    def place_futures_market_buy(self, symbol_fut: str, qty: float) -> str:
        raise NotImplementedError("BitgetAdapter 不支持合约操作，请使用 CrossExchangeAdapter")

    def get_futures_position(self, symbol_fut: str) -> float:
        raise NotImplementedError("BitgetAdapter 不支持合约操作，请使用 CrossExchangeAdapter")
