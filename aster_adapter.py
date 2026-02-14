"""Aster DEX 交易所适配器 —— 实现 ExchangeAdapter 的全部方法。

Aster API 与 Binance 高度兼容（相同 HMAC-SHA256 签名、相同 X-MBX-APIKEY header），
主要差异在于域名和部分路径版本号。

现货 REST: https://sapi.asterdex.com  (/api/v1/*)
合约 REST: https://fapi.asterdex.com  (/fapi/v1/* + /fapi/v2/*)
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import threading
import time
from collections import deque
from typing import Optional
from urllib.parse import urlencode

import requests

from arbitrage_bot import ExchangeAdapter

logger = logging.getLogger(__name__)

# Aster API error codes（与 Binance 相同体系）
_ERR_UNKNOWN_ORDER = -2011  # 订单不存在（已成交或已撤销）
_ERR_ORDER_NOT_FOUND = -2013  # 订单查询不到

_SPOT_BASE = "https://sapi.asterdex.com"
_FAPI_BASE = "https://fapi.asterdex.com"


class RateLimiter:
    """简易滑动窗口限速器。"""

    def __init__(self, max_weight: int = 1200, window_sec: int = 60) -> None:
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

    def current_usage(self) -> int:
        with self._lock:
            self._cleanup(time.time())
            return sum(w for _, w in self._requests)

    def wait_if_needed(self, weight: int = 1) -> None:
        """如果接近限速阈值，等到窗口释放足够额度。"""
        threshold = self._max * 0.8
        while True:
            with self._lock:
                now = time.time()
                self._cleanup(now)
                usage = sum(w for _, w in self._requests)
                if usage <= threshold:
                    self._requests.append((now, weight))
                    return
            logger.warning("接近限速阈值 (%d/%d)，暂停 0.5s", usage, self._max)
            time.sleep(0.5)


def _fmt_decimal(value: float, precision: int = 8) -> str:
    """格式化浮点数为固定精度字符串，避免科学计数法和尾部精度噪音。"""
    return f"{value:.{precision}f}".rstrip("0").rstrip(".")


class AsterAdapter(ExchangeAdapter):
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        price_cache=None,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret

        # ── 可选的 WS 价格缓存 ──
        self.price_cache = price_cache

        # ── 限速器 ──
        self._spot_limiter = RateLimiter(max_weight=1200, window_sec=60)
        self._fut_limiter = RateLimiter(max_weight=2400, window_sec=60)

        self._last_hedge_avg_price: float | None = None

        # 现货 session
        self._spot_session = requests.Session()
        self._spot_session.headers.update({"X-MBX-APIKEY": api_key})

        # 合约 session
        self._fut_session = requests.Session()
        self._fut_session.headers.update({"X-MBX-APIKEY": api_key})

        logger.info("AsterAdapter 初始化完成 (sapi + fapi 模式)")

    # ── 签名 ─────────────────────────────────────────────────

    def _sign(self, params: dict) -> dict:
        """为请求参数添加 timestamp 和 signature (HMAC-SHA256)。"""
        params["timestamp"] = int(time.time() * 1000)
        query = urlencode(params)
        signature = hmac.new(
            self._api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()
        params["signature"] = signature
        return params

    # ── 现货 REST 请求 ────────────────────────────────────────

    def _spot_request(self, method: str, path: str, params: dict | None = None, signed: bool = True) -> dict:
        """发送请求到 Aster 现货端点。"""
        params = params or {}
        if signed:
            self._sign(params)
        url = f"{_SPOT_BASE}{path}"
        resp = self._spot_session.request(method, url, params=params, timeout=10)
        data = resp.json()
        if resp.status_code != 200:
            code = data.get("code", resp.status_code)
            msg = data.get("msg", "unknown error")
            raise Exception(f"Aster Spot ({resp.status_code}, {code}, '{msg}')")
        return data

    # ── 合约 REST 请求 ────────────────────────────────────────

    def _fapi_request(self, method: str, path: str, params: dict | None = None, signed: bool = True) -> dict:
        """发送签名请求到 Aster fapi 端点。"""
        params = params or {}
        if signed:
            self._sign(params)
        url = f"{_FAPI_BASE}{path}"
        resp = self._fut_session.request(method, url, params=params, timeout=10)
        data = resp.json()
        if resp.status_code != 200:
            code = data.get("code", resp.status_code)
            msg = data.get("msg", "unknown error")
            raise Exception(f"Aster Futures ({resp.status_code}, {code}, '{msg}')")
        return data

    def _fapi_get_public(self, path: str, params: dict | None = None) -> dict:
        """fapi 公开行情接口（无需签名）。"""
        url = f"{_FAPI_BASE}{path}"
        resp = self._fut_session.get(url, params=params or {}, timeout=10)
        data = resp.json()
        if resp.status_code != 200:
            raise Exception(f"Aster fapi 请求失败: {resp.status_code} {data}")
        return data

    # ── 合约均价兜底 ─────────────────────────────────────────

    def _resolve_futures_avg_price(self, symbol_fut: str, order_id: str) -> float | None:
        """avgPrice=0 时追加查询订单明细兜底拿成交均价。"""
        for i in range(3):
            try:
                resp = self._fapi_request("GET", "/fapi/v1/order", {
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
        """启动前校验：确认交易对存在，返回 tick_size / lot_size。"""
        result = {}

        # 现货 exchangeInfo
        try:
            self._spot_limiter.wait_if_needed(weight=10)
            info = self._spot_request("GET", "/api/v1/exchangeInfo", {"symbol": symbol_spot}, signed=False)
            for sym in info.get("symbols", []):
                if sym["symbol"] == symbol_spot:
                    for f in sym.get("filters", []):
                        if f["filterType"] == "PRICE_FILTER":
                            result["spot_tick_size"] = float(f["tickSize"])
                        elif f["filterType"] == "LOT_SIZE":
                            result["spot_lot_size"] = float(f["stepSize"])
                    logger.info("Aster 现货 %s 校验通过: tick=%.8f, lot=%.8f",
                                symbol_spot,
                                result.get("spot_tick_size", 0),
                                result.get("spot_lot_size", 0))
                    break
            else:
                logger.warning("Aster 现货 exchangeInfo 中未找到 %s", symbol_spot)
        except Exception:
            logger.exception("获取 Aster 现货 exchangeInfo 失败")

        # 合约 exchangeInfo
        try:
            self._fut_limiter.wait_if_needed(weight=10)
            info = self._fapi_get_public("/fapi/v1/exchangeInfo")
            for sym in info.get("symbols", []):
                if sym["symbol"] == symbol_fut:
                    for f in sym.get("filters", []):
                        if f["filterType"] == "PRICE_FILTER":
                            result["fut_tick_size"] = float(f["tickSize"])
                        elif f["filterType"] == "LOT_SIZE":
                            result["fut_lot_size"] = float(f["stepSize"])
                    logger.info("Aster 合约 %s 校验通过: tick=%.8f, lot=%.8f",
                                symbol_fut,
                                result.get("fut_tick_size", 0),
                                result.get("fut_lot_size", 0))
                    break
            else:
                logger.warning("Aster 合约 exchangeInfo 中未找到 %s", symbol_fut)
        except Exception:
            logger.exception("获取 Aster 合约 exchangeInfo 失败")

        return result

    # ── 行情 ──────────────────────────────────────────────────

    def get_futures_best_bid(self, symbol_fut: str) -> float:
        if self.price_cache is not None:
            bid = self.price_cache.get_futures_bid()
            if bid is not None and not self.price_cache.is_stale():
                return bid
            logger.debug("WS 价格缓存过期或为空，回退到 REST")

        self._fut_limiter.wait_if_needed(weight=2)
        resp = self._fapi_get_public("/fapi/v1/ticker/bookTicker", {"symbol": symbol_fut})
        return float(resp["bidPrice"])

    def get_futures_best_ask(self, symbol_fut: str) -> float:
        if self.price_cache is not None:
            ask = self.price_cache.get_futures_ask()
            if ask is not None and not self.price_cache.is_stale():
                return ask
            logger.debug("WS 价格缓存过期或为空，回退到 REST")

        self._fut_limiter.wait_if_needed(weight=2)
        resp = self._fapi_get_public("/fapi/v1/ticker/bookTicker", {"symbol": symbol_fut})
        return float(resp["askPrice"])

    def get_spot_depth(self, symbol_spot: str, levels: int = 5) -> list[tuple[float, float]]:
        """返回现货买盘深度，优先 WS 缓存，回退 REST。"""
        if self.price_cache is not None:
            bids = self.price_cache.get_spot_bids(n=levels)
            if bids and not self.price_cache.is_spot_depth_stale():
                return bids
            logger.debug("WS 现货深度缓存过期或为空，回退到 REST")

        self._spot_limiter.wait_if_needed(weight=5)
        resp = self._spot_request("GET", "/api/v1/depth", {"symbol": symbol_spot, "limit": levels}, signed=False)
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
        resp = self._spot_request("GET", "/api/v1/depth", {"symbol": symbol_spot, "limit": levels}, signed=False)
        asks = [(float(p), float(q)) for p, q in resp.get("asks", [])]
        return asks

    def get_spot_open_bid_order(self, symbol_spot: str) -> Optional[dict]:
        self._spot_limiter.wait_if_needed(weight=3)
        orders = self._spot_request("GET", "/api/v1/openOrders", {"symbol": symbol_spot})
        if not isinstance(orders, list):
            return None
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
        resp = self._spot_request("POST", "/api/v1/order", {
            "symbol": symbol_spot,
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": _fmt_decimal(qty),
            "price": _fmt_decimal(price),
        })
        order_id = str(resp["orderId"])
        logger.info("Aster 现货限价买单已下: order_id=%s, price=%s, qty=%s", order_id, price, qty)
        return order_id

    def place_spot_limit_sell(self, symbol_spot: str, price: float, qty: float) -> str:
        self._spot_limiter.wait_if_needed(weight=1)
        resp = self._spot_request("POST", "/api/v1/order", {
            "symbol": symbol_spot,
            "side": "SELL",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": _fmt_decimal(qty),
            "price": _fmt_decimal(price),
        })
        order_id = str(resp["orderId"])
        logger.info("Aster 现货限价卖单已下: order_id=%s, price=%s, qty=%s", order_id, price, qty)
        return order_id

    def cancel_order(self, symbol: str, order_id: str) -> None:
        self._spot_limiter.wait_if_needed(weight=1)
        try:
            self._spot_request("DELETE", "/api/v1/order", {
                "symbol": symbol,
                "orderId": int(order_id),
            })
            logger.info("Aster 撤单成功: order_id=%s", order_id)
        except Exception as e:
            err_str = str(e)
            if str(_ERR_UNKNOWN_ORDER) in err_str:
                logger.warning("撤单时订单已不存在 (可能已成交): order_id=%s", order_id)
            else:
                raise

    def get_order_filled_qty(self, symbol: str, order_id: str) -> float:
        self._spot_limiter.wait_if_needed(weight=2)
        try:
            resp = self._spot_request("GET", "/api/v1/order", {
                "symbol": symbol,
                "orderId": int(order_id),
            })
            return float(resp["executedQty"])
        except Exception as e:
            err_str = str(e)
            if str(_ERR_ORDER_NOT_FOUND) in err_str:
                logger.warning("Aster 查单 -2013: order_id=%s 不存在，返回哨兵值 -1", order_id)
                return -1.0
            raise

    def place_futures_market_sell(self, symbol_fut: str, qty: float) -> str:
        self._fut_limiter.wait_if_needed(weight=1)
        resp = self._fapi_request("POST", "/fapi/v1/order", {
            "symbol": symbol_fut,
            "side": "SELL",
            "type": "MARKET",
            "quantity": _fmt_decimal(qty),
        })
        order_id = str(resp["orderId"])
        avg_price = float(resp.get("avgPrice", 0)) if resp.get("avgPrice") else None
        if not avg_price or avg_price <= 0:
            avg_price = self._resolve_futures_avg_price(symbol_fut, order_id)
        logger.info("Aster 合约市价卖出已下: order_id=%s, qty=%s, avg_price=%s", order_id, qty, avg_price)
        self._last_hedge_avg_price = avg_price
        return order_id

    def place_futures_market_buy(self, symbol_fut: str, qty: float) -> str:
        self._fut_limiter.wait_if_needed(weight=1)
        resp = self._fapi_request("POST", "/fapi/v1/order", {
            "symbol": symbol_fut,
            "side": "BUY",
            "type": "MARKET",
            "quantity": _fmt_decimal(qty),
        })
        order_id = str(resp["orderId"])
        avg_price = float(resp.get("avgPrice", 0)) if resp.get("avgPrice") else None
        if not avg_price or avg_price <= 0:
            avg_price = self._resolve_futures_avg_price(symbol_fut, order_id)
        logger.info("Aster 合约市价买入已下: order_id=%s, qty=%s, avg_price=%s", order_id, qty, avg_price)
        self._last_hedge_avg_price = avg_price
        return order_id

    @property
    def last_hedge_avg_price(self) -> float | None:
        """最近一次对冲的成交均价。"""
        return getattr(self, "_last_hedge_avg_price", None)

    # ── 账户持仓查询 ──────────────────────────────────────────

    def get_spot_balance(self, asset: str) -> float:
        """查询 Aster 现货余额（现货账户 + 合约账户余额之和）。"""
        total = 0.0

        # 1) 现货账户
        try:
            self._spot_limiter.wait_if_needed(weight=10)
            account = self._spot_request("GET", "/api/v1/account")
            for b in account.get("balances", []):
                if b.get("asset") == asset:
                    total += float(b.get("free", 0)) + float(b.get("locked", 0))
                    break
        except Exception:
            logger.warning("查询 Aster 现货账户余额失败: asset=%s", asset)

        # 2) 合约账户余额
        try:
            self._fut_limiter.wait_if_needed(weight=5)
            resp = self._fapi_request("GET", "/fapi/v2/balance")
            if isinstance(resp, list):
                for item in resp:
                    if item.get("asset") == asset:
                        total += float(item.get("balance", 0))
                        break
        except Exception:
            logger.warning("查询 Aster 合约账户余额失败: asset=%s", asset)

        return total if total > 0 else (0.0 if total == 0.0 else -1.0)

    def get_earn_balance(self, asset: str) -> float:
        """Aster 暂不支持理财产品查询，返回 0。"""
        return 0.0

    def get_futures_position(self, symbol_fut: str) -> float:
        """查询 Aster 永续合约持仓量（负数=空头）。"""
        self._fut_limiter.wait_if_needed(weight=5)
        try:
            resp = self._fapi_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol_fut})
            if isinstance(resp, list):
                for item in resp:
                    if item.get("symbol") == symbol_fut:
                        return float(item.get("positionAmt", 0))
                return 0.0
            return float(resp.get("positionAmt", 0))
        except Exception:
            logger.exception("查询 Aster 合约持仓失败: symbol=%s", symbol_fut)
            return 0.0

    def get_futures_account_info(self, symbol_fut: str) -> dict:
        """查询 Aster 合约账户详情：保证金、可用余额、强平价、可开仓位等。"""
        result: dict = {}
        try:
            self._fut_limiter.wait_if_needed(weight=5)
            acc = self._fapi_request("GET", "/fapi/v2/account")
            if isinstance(acc, dict):
                result["wallet_balance"] = float(acc.get("totalWalletBalance", 0))
                result["unrealized_pnl"] = float(acc.get("totalUnrealizedProfit", 0))
                result["margin_balance"] = float(acc.get("totalMarginBalance", 0))
                result["available_balance"] = float(acc.get("availableBalance", 0))
                result["max_withdraw"] = float(acc.get("maxWithdrawAmount", 0))
                # 从 positions 中提取当前币对的持仓保证金
                for p in acc.get("positions", []):
                    if p.get("symbol") == symbol_fut and float(p.get("positionAmt", 0)) != 0:
                        result["position_initial_margin"] = float(p.get("positionInitialMargin", 0))
                        result["maint_margin"] = float(p.get("maintMargin", 0))
                        result["leverage"] = int(p.get("leverage", 1))
                        result["entry_price"] = float(p.get("entryPrice", 0))
                        result["notional"] = abs(float(p.get("notional", 0)))
                        break
        except Exception:
            logger.warning("查询 Aster 合约账户信息失败")

        try:
            self._fut_limiter.wait_if_needed(weight=5)
            resp = self._fapi_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol_fut})
            if isinstance(resp, list):
                for item in resp:
                    if item.get("symbol") == symbol_fut:
                        result["liquidation_price"] = float(item.get("liquidationPrice", 0))
                        result["mark_price"] = float(item.get("markPrice", 0))
                        break
        except Exception:
            logger.warning("查询 Aster 合约 positionRisk 失败")

        # 计算距离强平的百分比和可开仓量
        mark = result.get("mark_price", 0)
        liq = result.get("liquidation_price", 0)
        if mark > 0 and liq > 0:
            result["liq_distance_pct"] = round(abs(liq - mark) / mark * 100, 2)
        avail = result.get("available_balance", 0)
        leverage = result.get("leverage", 1)
        if mark > 0 and avail > 0 and leverage > 0:
            result["max_open_qty"] = round(avail * leverage / mark, 2)
            result["max_open_notional"] = round(avail * leverage, 2)

        return result

    def internal_transfer(self, asset: str, amount: float, direction: str) -> dict:
        """在现货和合约账户之间划转资产。

        direction:
            'to_spot'   — 合约 → 现货 (FUTURE_SPOT)
            'to_future' — 现货 → 合约 (SPOT_FUTURE)

        Returns: {"tranId": ..., "status": "SUCCESS"} or raises Exception.
        """
        import uuid

        kind_map = {
            "to_spot": "FUTURE_SPOT",
            "to_future": "SPOT_FUTURE",
        }
        kind_type = kind_map.get(direction)
        if not kind_type:
            raise ValueError(f"无效方向: {direction}，应为 'to_spot' 或 'to_future'")

        self._fut_limiter.wait_if_needed(weight=5)
        resp = self._fapi_request("POST", "/fapi/v1/asset/wallet/transfer", {
            "asset": asset,
            "amount": _fmt_decimal(amount),
            "clientTranId": str(uuid.uuid4()),
            "kindType": kind_type,
        })
        logger.info("Aster 内部划转完成: %s %s %s → %s", amount, asset, direction, resp)
        return resp
