"""Binance 交易所适配器 —— 实现 ExchangeAdapter 的全部方法。

使用官方 SDK:
  - binance-connector (现货)
合约通过 Portfolio Margin API (papi) 直接 REST 调用。
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from collections import deque
from typing import Optional
from urllib.parse import urlencode

import requests
from binance.spot import Spot

from arbitrage_bot import ExchangeAdapter

logger = logging.getLogger(__name__)

# Binance API error codes
_ERR_UNKNOWN_ORDER = -2011  # 订单不存在（已成交或已撤销）
_ERR_ORDER_NOT_FOUND = -2013  # 订单查询不到

_PAPI_BASE = "https://papi.binance.com"
_FAPI_BASE = "https://fapi.binance.com"


class RateLimiter:
    """简易滑动窗口限速器。"""

    def __init__(self, max_weight: int = 1200, window_sec: int = 60) -> None:
        self._max = max_weight
        self._window = window_sec
        self._requests: deque[tuple[float, int]] = deque()

    def record(self, weight: int = 1) -> None:
        now = time.time()
        self._requests.append((now, weight))
        self._cleanup(now)

    def _cleanup(self, now: float) -> None:
        cutoff = now - self._window
        while self._requests and self._requests[0][0] < cutoff:
            self._requests.popleft()

    def current_usage(self) -> int:
        self._cleanup(time.time())
        return sum(w for _, w in self._requests)

    def should_throttle(self, threshold: float = 0.8) -> bool:
        return self.current_usage() > self._max * threshold

    def wait_if_needed(self, weight: int = 1) -> None:
        """如果接近限速阈值，等到窗口释放足够额度。"""
        while self.should_throttle():
            logger.warning("接近限速阈值 (%d/%d)，暂停 0.5s", self.current_usage(), self._max)
            time.sleep(0.5)
        self.record(weight)


def _fmt_decimal(value: float, precision: int = 8) -> str:
    """格式化浮点数为固定精度字符串，避免科学计数法和尾部精度噪音。"""
    return f"{value:.{precision}f}".rstrip("0").rstrip(".")


class BinanceAdapter(ExchangeAdapter):
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        price_cache=None,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret

        # ── 现货客户端 ──
        self.spot = Spot(api_key=api_key, api_secret=api_secret)

        # ── 可选的 WS 价格缓存 ──
        self.price_cache = price_cache

        # ── 限速器 ──
        self._spot_limiter = RateLimiter(max_weight=1200, window_sec=60)
        self._fut_limiter = RateLimiter(max_weight=6000, window_sec=60)

        self._last_hedge_avg_price: float | None = None
        self._session = requests.Session()
        self._session.headers.update({"X-MBX-APIKEY": api_key})
        logger.info("BinanceAdapter 初始化完成 (papi 统一账户模式)")

    # ── PAPI 签名与请求 ─────────────────────────────────────

    def _sign(self, params: dict) -> dict:
        """为请求参数添加 timestamp 和 signature。"""
        params["timestamp"] = int(time.time() * 1000)
        query = urlencode(params)
        signature = hmac.new(
            self._api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        params["signature"] = signature
        return params

    def _papi_request(self, method: str, path: str, params: dict | None = None) -> dict:
        """发送签名请求到 papi 端点。"""
        params = params or {}
        self._sign(params)
        url = f"{_PAPI_BASE}{path}"
        resp = self._session.request(method, url, params=params, timeout=10)
        data = resp.json()
        if resp.status_code != 200:
            code = data.get("code", resp.status_code)
            msg = data.get("msg", "unknown error")
            raise Exception(f"({resp.status_code}, {code}, '{msg}')")
        return data

    def _fapi_get(self, path: str, params: dict | None = None) -> dict:
        """fapi 公开行情接口（无需签名）。"""
        url = f"{_FAPI_BASE}{path}"
        resp = self._session.get(url, params=params or {}, timeout=10)
        data = resp.json()
        if resp.status_code != 200:
            raise Exception(f"fapi 请求失败: {resp.status_code} {data}")
        return data

    # ── 合约均价兜底 ─────────────────────────────────────────

    def _resolve_futures_avg_price(self, symbol_fut: str, order_id: str) -> float | None:
        """avgPrice=0 时追加查询订单明细兜底拿成交均价。"""
        for i in range(3):
            try:
                resp = self._papi_request("GET", "/papi/v1/um/order", {
                    "symbol": symbol_fut,
                    "orderId": order_id,
                })
                avg_price = float(resp.get("avgPrice", 0) or 0)
                if avg_price > 0:
                    return avg_price
                executed = float(resp.get("executedQty", 0) or 0)
                cum_quote = float(resp.get("cumQuote", 0) or 0)
                if executed > 0 and cum_quote > 0:
                    return cum_quote / executed
            except Exception:
                logger.debug("回查合约均价失败: order_id=%s, retry=%d", order_id, i + 1)
            time.sleep(0.05 * (i + 1))
        return None

    def preflight_check(self, symbol_spot: str, symbol_fut: str) -> dict:
        """启动前校验：确认交易对存在，返回 tick_size / lot_size 供参考。

        Returns:
            {"spot_tick_size": float, "spot_lot_size": float,
             "fut_tick_size": float, "fut_lot_size": float}
        """
        result = {}

        # 现货 exchangeInfo
        try:
            info = self.spot.exchange_info(symbol=symbol_spot)
            for sym in info.get("symbols", []):
                if sym["symbol"] == symbol_spot:
                    for f in sym.get("filters", []):
                        if f["filterType"] == "PRICE_FILTER":
                            result["spot_tick_size"] = float(f["tickSize"])
                        elif f["filterType"] == "LOT_SIZE":
                            result["spot_lot_size"] = float(f["stepSize"])
                    logger.info("现货 %s 校验通过: tick=%.8f, lot=%.8f",
                                symbol_spot,
                                result.get("spot_tick_size", 0),
                                result.get("spot_lot_size", 0))
                    break
            else:
                logger.warning("现货 exchangeInfo 中未找到 %s", symbol_spot)
        except Exception:
            logger.exception("获取现货 exchangeInfo 失败")

        # 合约 exchangeInfo（fapi 公开接口仍可用于行情数据）
        try:
            info = self._fapi_get("/fapi/v1/exchangeInfo")
            for sym in info.get("symbols", []):
                if sym["symbol"] == symbol_fut:
                    for f in sym.get("filters", []):
                        if f["filterType"] == "PRICE_FILTER":
                            result["fut_tick_size"] = float(f["tickSize"])
                        elif f["filterType"] == "LOT_SIZE":
                            result["fut_lot_size"] = float(f["stepSize"])
                    logger.info("合约 %s 校验通过: tick=%.8f, lot=%.8f",
                                symbol_fut,
                                result.get("fut_tick_size", 0),
                                result.get("fut_lot_size", 0))
                    break
            else:
                logger.warning("合约 exchangeInfo 中未找到 %s", symbol_fut)
        except Exception:
            logger.exception("获取合约 exchangeInfo 失败")

        return result

    # ── 行情 ──────────────────────────────────────────────────

    def get_futures_best_bid(self, symbol_fut: str) -> float:
        # 优先从 WS 缓存读取
        if self.price_cache is not None:
            bid = self.price_cache.get_futures_bid()
            if bid is not None and not self.price_cache.is_stale():
                return bid
            logger.debug("WS 价格缓存过期或为空，回退到 REST")

        self._fut_limiter.wait_if_needed(weight=2)
        resp = self._fapi_get("/fapi/v1/ticker/bookTicker", {"symbol": symbol_fut})
        return float(resp["bidPrice"])

    def get_futures_best_ask(self, symbol_fut: str) -> float:
        if self.price_cache is not None:
            ask = self.price_cache.get_futures_ask()
            if ask is not None and not self.price_cache.is_stale():
                return ask
            logger.debug("WS 价格缓存过期或为空，回退到 REST")

        self._fut_limiter.wait_if_needed(weight=2)
        resp = self._fapi_get("/fapi/v1/ticker/bookTicker", {"symbol": symbol_fut})
        return float(resp["askPrice"])

    def get_spot_depth(self, symbol_spot: str, levels: int = 5) -> list[tuple[float, float]]:
        """返回现货买盘深度，优先 WS 缓存，回退 REST。"""
        if self.price_cache is not None:
            bids = self.price_cache.get_spot_bids(n=levels)
            if bids and not self.price_cache.is_spot_depth_stale():
                return bids
            logger.debug("WS 现货深度缓存过期或为空，回退到 REST")

        self._spot_limiter.wait_if_needed(weight=5)
        resp = self.spot.depth(symbol=symbol_spot, limit=levels)
        bids = [(float(p), float(q)) for p, q in resp.get("bids", [])]
        return bids

    def get_spot_asks(self, symbol_spot: str, levels: int = 5) -> list[tuple[float, float]]:
        """返回现货卖盘深度，优先 WS 缓存，回退 REST。"""
        if self.price_cache is not None:
            asks = self.price_cache.get_spot_asks(n=levels)
            if asks and not self.price_cache.is_spot_depth_stale():
                return asks
            logger.debug("WS 现货深度缓存过期或为空，回退到 REST")

        self._spot_limiter.wait_if_needed(weight=5)
        resp = self.spot.depth(symbol=symbol_spot, limit=levels)
        asks = [(float(p), float(q)) for p, q in resp.get("asks", [])]
        return asks

    def get_spot_open_bid_order(self, symbol_spot: str) -> Optional[dict]:
        self._spot_limiter.wait_if_needed(weight=3)
        orders = self.spot.get_open_orders(symbol=symbol_spot)
        for o in orders:
            if o["side"] == "BUY" and o["status"] in ("NEW", "PARTIALLY_FILLED"):
                return {
                    "order_id": str(o["orderId"]),
                    "price": float(o["price"]),
                    "qty": float(o["origQty"]),
                    "filled_qty": float(o["executedQty"]),
                }
        return None

    # ── 下单 ──────────────────────────────────────────────────

    def place_spot_limit_buy(self, symbol_spot: str, price: float, qty: float) -> str:
        self._spot_limiter.wait_if_needed(weight=1)
        resp = self.spot.new_order(
            symbol=symbol_spot,
            side="BUY",
            type="LIMIT",
            timeInForce="GTC",
            quantity=_fmt_decimal(qty),
            price=_fmt_decimal(price),
        )
        order_id = str(resp["orderId"])
        logger.info("现货限价买单已下: order_id=%s, price=%s, qty=%s", order_id, price, qty)
        return order_id

    def place_spot_limit_sell(self, symbol_spot: str, price: float, qty: float) -> str:
        self._spot_limiter.wait_if_needed(weight=1)
        resp = self.spot.new_order(
            symbol=symbol_spot,
            side="SELL",
            type="LIMIT",
            timeInForce="GTC",
            quantity=_fmt_decimal(qty),
            price=_fmt_decimal(price),
        )
        order_id = str(resp["orderId"])
        logger.info("现货限价卖单已下: order_id=%s, price=%s, qty=%s", order_id, price, qty)
        return order_id

    def cancel_order(self, symbol: str, order_id: str) -> None:
        self._spot_limiter.wait_if_needed(weight=1)
        try:
            self.spot.cancel_order(symbol=symbol, orderId=int(order_id))
            logger.info("撤单成功: order_id=%s", order_id)
        except Exception as e:
            err_str = str(e)
            if str(_ERR_UNKNOWN_ORDER) in err_str:
                # 订单已成交或已被撤销 —— 竞态条件，不算错误
                logger.warning("撤单时订单已不存在 (可能已成交): order_id=%s", order_id)
            else:
                raise

    def get_order_filled_qty(self, symbol: str, order_id: str) -> float:
        self._spot_limiter.wait_if_needed(weight=2)
        try:
            resp = self.spot.get_order(symbol=symbol, orderId=int(order_id))
            return float(resp["executedQty"])
        except Exception as e:
            err_str = str(e)
            if str(_ERR_ORDER_NOT_FOUND) in err_str:
                # 订单查不到 —— 可能被清理或不存在
                # 返回 -1 作为哨兵值，让调用方决定如何处理
                logger.warning("查单 -2013: order_id=%s 不存在，返回哨兵值 -1", order_id)
                return -1.0
            raise

    def place_futures_market_sell(self, symbol_fut: str, qty: float) -> str:
        self._fut_limiter.wait_if_needed(weight=1)
        resp = self._papi_request("POST", "/papi/v1/um/order", {
            "symbol": symbol_fut,
            "side": "SELL",
            "type": "MARKET",
            "quantity": _fmt_decimal(qty),
        })
        order_id = str(resp["orderId"])
        # 尝试获取成交均价
        avg_price = float(resp.get("avgPrice", 0)) if resp.get("avgPrice") else None
        if not avg_price or avg_price <= 0:
            avg_price = self._resolve_futures_avg_price(symbol_fut, order_id)
        logger.info("合约市价卖出已下: order_id=%s, qty=%s, avg_price=%s", order_id, qty, avg_price)
        # 把均价存到实例上供外部读取
        self._last_hedge_avg_price = avg_price
        return order_id

    def place_futures_market_buy(self, symbol_fut: str, qty: float) -> str:
        self._fut_limiter.wait_if_needed(weight=1)
        resp = self._papi_request("POST", "/papi/v1/um/order", {
            "symbol": symbol_fut,
            "side": "BUY",
            "type": "MARKET",
            "quantity": _fmt_decimal(qty),
        })
        order_id = str(resp["orderId"])
        avg_price = float(resp.get("avgPrice", 0)) if resp.get("avgPrice") else None
        if not avg_price or avg_price <= 0:
            avg_price = self._resolve_futures_avg_price(symbol_fut, order_id)
        logger.info("合约市价买入已下: order_id=%s, qty=%s, avg_price=%s", order_id, qty, avg_price)
        self._last_hedge_avg_price = avg_price
        return order_id

    @property
    def last_hedge_avg_price(self) -> float | None:
        """最近一次对冲的成交均价。"""
        return getattr(self, "_last_hedge_avg_price", None)

    # ── 账户持仓查询 ──────────────────────────────────────────

    def get_spot_balance(self, asset: str) -> float:
        """查询现货余额，同时检查统一账户(papi)和普通现货钱包。

        注意：Portfolio Margin 模式下，现货余额可能已包含在 papi 返回中，
        此时两处相加可能导致重复计算。请根据实际账户类型确认。
        """
        total = 0.0

        # 1) 统一账户 (Portfolio Margin)
        self._spot_limiter.wait_if_needed(weight=20)
        try:
            resp = self._papi_request("GET", "/papi/v1/balance", {"asset": asset})
            if isinstance(resp, list):
                for item in resp:
                    if item.get("asset") == asset:
                        total += float(item.get("crossMarginFree", 0))
                        total += float(item.get("crossMarginLocked", 0))
                        break
            else:
                total += float(resp.get("crossMarginFree", 0))
                total += float(resp.get("crossMarginLocked", 0))
        except Exception:
            logger.exception("查询统一账户余额失败: asset=%s", asset)

        # 2) 普通现货钱包 (/api/v3/account)
        try:
            self._spot_limiter.wait_if_needed(weight=20)
            account = self.spot.account()
            for b in account.get("balances", []):
                if b.get("asset") == asset:
                    total += float(b.get("free", 0))
                    total += float(b.get("locked", 0))
                    break
        except Exception:
            logger.exception("查询普通现货余额失败: asset=%s", asset)

        return total if total >= 0 else -1.0

    def get_earn_balance(self, asset: str) -> float:
        """查询活期理财余额（Simple Earn Flexible）。"""
        self._spot_limiter.wait_if_needed(weight=1)
        try:
            params = {"asset": asset, "size": 100}
            self._sign(params)
            url = "https://api.binance.com/sapi/v1/simple-earn/flexible/position"
            resp = self._session.get(url, params=params, timeout=10)
            data = resp.json()
            if resp.status_code != 200:
                logger.warning("查询理财余额失败: %s", data)
                return 0.0
            total = 0.0
            for row in data.get("rows", []):
                if row.get("asset") == asset:
                    total += float(row.get("totalAmount", 0))
            return total
        except Exception:
            logger.exception("查询理财余额失败: asset=%s", asset)
            return 0.0

    def get_futures_position(self, symbol_fut: str) -> float:
        """查询永续合约持仓量（负数=空头）。"""
        self._fut_limiter.wait_if_needed(weight=5)
        try:
            resp = self._papi_request("GET", "/papi/v1/um/positionRisk", {"symbol": symbol_fut})
            if isinstance(resp, list):
                for item in resp:
                    if item.get("symbol") == symbol_fut:
                        return float(item.get("positionAmt", 0))
                return 0.0
            return float(resp.get("positionAmt", 0))
        except Exception:
            logger.exception("查询合约持仓失败: symbol=%s", symbol_fut)
            return 0.0
