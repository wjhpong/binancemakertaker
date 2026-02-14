"""跨所复合适配器 —— 将现货和合约操作分别委托给不同交易所的 adapter。

用法（run.py）：
    spot_adapter = GateAdapter(...)
    futures_adapter = BinanceAdapter(...)
    adapter = CrossExchangeAdapter(spot_adapter, futures_adapter)
    bot = SpotFuturesArbitrageBot(adapter=adapter, ...)
"""

from __future__ import annotations

import logging
from typing import Optional

from arbitrage_bot import ExchangeAdapter

logger = logging.getLogger(__name__)


class CrossExchangeAdapter(ExchangeAdapter):
    """复合 adapter：现货操作委托给 spot_adapter，合约操作委托给 futures_adapter。

    实现了与单所 adapter 完全相同的 ExchangeAdapter 接口，
    对 bot 核心逻辑（arbitrage_bot / fill_handler）完全透明。
    """

    def __init__(
        self,
        spot_adapter: ExchangeAdapter,
        futures_adapter: ExchangeAdapter,
    ) -> None:
        self.spot = spot_adapter
        self.futures = futures_adapter
        self._last_hedge_avg_price: float | None = None

    # ── 现货方法 → 委托给 self.spot ────────────────────────────

    def get_spot_depth(self, symbol_spot: str, levels: int = 5) -> list[tuple[float, float]]:
        return self.spot.get_spot_depth(symbol_spot, levels)

    def get_spot_asks(self, symbol_spot: str, levels: int = 5) -> list[tuple[float, float]]:
        return self.spot.get_spot_asks(symbol_spot, levels)

    def get_spot_open_bid_order(self, symbol_spot: str) -> Optional[dict]:
        return self.spot.get_spot_open_bid_order(symbol_spot)

    def place_spot_limit_buy(self, symbol_spot: str, price: float, qty: float) -> str:
        return self.spot.place_spot_limit_buy(symbol_spot, price, qty)

    def place_spot_limit_sell(self, symbol_spot: str, price: float, qty: float) -> str:
        return self.spot.place_spot_limit_sell(symbol_spot, price, qty)

    def get_spot_balance(self, asset: str) -> float:
        return self.spot.get_spot_balance(asset)

    def get_earn_balance(self, asset: str) -> float:
        return self.spot.get_earn_balance(asset)

    # ── 合约方法 → 委托给 self.futures ─────────────────────────

    def get_futures_best_bid(self, symbol_fut: str) -> float:
        return self.futures.get_futures_best_bid(symbol_fut)

    def get_futures_best_ask(self, symbol_fut: str) -> float:
        return self.futures.get_futures_best_ask(symbol_fut)

    def place_futures_market_sell(self, symbol_fut: str, qty: float) -> str:
        oid = self.futures.place_futures_market_sell(symbol_fut, qty)
        self._last_hedge_avg_price = getattr(self.futures, "last_hedge_avg_price", None)
        return oid

    def place_futures_market_buy(self, symbol_fut: str, qty: float) -> str:
        oid = self.futures.place_futures_market_buy(symbol_fut, qty)
        self._last_hedge_avg_price = getattr(self.futures, "last_hedge_avg_price", None)
        return oid

    def get_futures_position(self, symbol_fut: str) -> float:
        return self.futures.get_futures_position(symbol_fut)

    def get_futures_account_info(self, symbol_fut: str) -> dict:
        return self.futures.get_futures_account_info(symbol_fut)

    # ── 现货路由方法（cancel / filled_qty 都是现货订单）─────────

    def cancel_order(self, symbol: str, order_id: str) -> None:
        return self.spot.cancel_order(symbol, order_id)

    def get_order_filled_qty(self, symbol: str, order_id: str) -> float:
        return self.spot.get_order_filled_qty(symbol, order_id)

    # ── Preflight 合并两所信息 ──────────────────────────────────

    def preflight_check(self, symbol_spot: str, symbol_fut: str) -> dict:
        result = {}
        try:
            spot_info = self.spot.preflight_check(symbol_spot, symbol_spot)
            if spot_info:
                result["spot_tick_size"] = spot_info.get("spot_tick_size")
                result["spot_lot_size"] = spot_info.get("spot_lot_size")
        except Exception:
            logger.warning("现货交易所 preflight_check 失败，跳过", exc_info=True)

        try:
            fut_info = self.futures.preflight_check(symbol_fut, symbol_fut)
            if fut_info:
                result["fut_tick_size"] = fut_info.get("fut_tick_size")
                result["fut_lot_size"] = fut_info.get("fut_lot_size")
        except Exception:
            logger.warning("合约交易所 preflight_check 失败，跳过", exc_info=True)

        return result

    # ── last_hedge_avg_price 透传 ──────────────────────────────

    @property
    def last_hedge_avg_price(self) -> float | None:
        return self._last_hedge_avg_price

    # 注意：不实现 internal_transfer()
    # control_server.py 已通过 hasattr() 检查，跨所模式下会正确返回 "不支持"
