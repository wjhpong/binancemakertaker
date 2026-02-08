#!/usr/bin/env python3
"""
同所期现套利开单逻辑（多档现货挂买单 -> 成交后合约市价卖出对冲）
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import queue

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FeeConfig:
    spot_maker: float = 0.000675    # VIP2 maker 0.0675%
    fut_taker: float = 0.00036     # VIP2 taker 0.036%
    min_spread_bps: float = -18.0  # 最小spread门槛 (bps)，可为负

    @property
    def min_spread(self) -> float:
        return self.min_spread_bps / 10000.0


@dataclass(frozen=True)
class StrategyConfig:
    symbol_spot: str
    symbol_fut: str
    tick_size_spot: float
    total_budget: float             # 总预算（币数量）
    budget_pct: float = 0.01        # 每轮总预算不超过总预算的 1%
    depth_ratio: float = 0.3        # 单档不超过该档深度的 30%
    min_order_qty: float = 0.00001
    lot_size: float = 0.00001
    poll_interval_sec: float = 0.2
    reprice_bps: float = 0.5
    max_retry: int = 3


class ExchangeAdapter:
    def get_futures_best_bid(self, symbol_fut: str) -> float:
        raise NotImplementedError

    def get_futures_best_ask(self, symbol_fut: str) -> float:
        raise NotImplementedError

    def get_spot_depth(self, symbol_spot: str, levels: int = 5) -> list[tuple[float, float]]:
        raise NotImplementedError

    def get_spot_asks(self, symbol_spot: str, levels: int = 5) -> list[tuple[float, float]]:
        raise NotImplementedError

    def get_spot_open_bid_order(self, symbol_spot: str) -> Optional[dict]:
        raise NotImplementedError

    def place_spot_limit_buy(self, symbol_spot: str, price: float, qty: float) -> str:
        raise NotImplementedError

    def place_spot_limit_sell(self, symbol_spot: str, price: float, qty: float) -> str:
        raise NotImplementedError

    def cancel_order(self, symbol: str, order_id: str) -> None:
        raise NotImplementedError

    def get_order_filled_qty(self, symbol: str, order_id: str) -> float:
        raise NotImplementedError

    def place_futures_market_sell(self, symbol_fut: str, qty: float) -> str:
        raise NotImplementedError

    def place_futures_market_buy(self, symbol_fut: str, qty: float) -> str:
        raise NotImplementedError


@dataclass
class LevelOrder:
    """跟踪单个档位的现货限价买单。"""
    level_idx: int       # 1..5 (买一..买五)
    order_id: str
    price: float
    qty: float
    accounted_qty: float = 0.0
    hedged_qty: float = 0.0


class SpotFuturesArbitrageBot:
    def __init__(
        self,
        adapter: ExchangeAdapter,
        fee: FeeConfig,
        cfg: StrategyConfig,
        trade_logger=None,
        fill_queue: Optional[queue.Queue] = None,
    ) -> None:
        from fill_handler import FillHandler

        self.adapter = adapter
        self.fee = fee
        self.cfg = cfg
        self.trade_logger = trade_logger
        self._state_lock = threading.RLock()

        # 多档挂单状态
        self._active_orders: dict[str, LevelOrder] = {}   # order_id → LevelOrder
        self._level_to_oid: dict[int, str] = {}            # level_idx → order_id

        self._running: bool = True
        self._paused: bool = False
        self._requote_all_levels: bool = False
        self._close_task_lock = threading.Lock()
        self._close_task_running: bool = False
        self._close_task_status: dict = {"running": False}
        self._close_pending_hedge: float = 0.0
        # 飞书通知器（可选）
        self._notifier = None

        # 成交检测 + 对冲（拆分到 fill_handler.py）
        self.fh = FillHandler(
            adapter=adapter,
            cfg=cfg,
            active_orders=self._active_orders,
            fill_queue=fill_queue,
            trade_logger=trade_logger,
        )
        self.fh._on_order_fully_filled = self._on_order_fully_filled

    @property
    def notifier(self):
        return self._notifier

    @notifier.setter
    def notifier(self, value) -> None:
        self._notifier = value
        self.fh.notifier = value

    def _on_order_fully_filled(self, oid: str, order: LevelOrder) -> None:
        """回调：FillHandler 检测到订单完全成交时清理索引。"""
        with self._state_lock:
            self._level_to_oid.pop(order.level_idx, None)
            self._active_orders.pop(oid, None)
            # 买一完全成交后，要求撤掉其余档位并重挂买一/买二/买三。
            if order.level_idx == 1:
                self._requote_all_levels = True

    @property
    def naked_exposure(self) -> float:
        return self.fh.naked_exposure

    @naked_exposure.setter
    def naked_exposure(self, value: float) -> None:
        self.fh.naked_exposure = value

    @property
    def _total_filled_usdt(self) -> float:
        return self.fh.total_filled_usdt

    @_total_filled_usdt.setter
    def _total_filled_usdt(self, value: float) -> None:
        self.fh.total_filled_usdt = value

    @property
    def _total_filled_base(self) -> float:
        return self.fh.total_filled_base

    @_total_filled_base.setter
    def _total_filled_base(self, value: float) -> None:
        self.fh.total_filled_base = value

    def stop(self) -> None:
        with self._state_lock:
            self._running = False

    @property
    def is_running(self) -> bool:
        with self._state_lock:
            return self._running

    def pause(self) -> None:
        """暂停挂单（撤销所有挂单，但不退出主循环）。"""
        with self._state_lock:
            self._paused = True
        logger.info("[CMD] 暂停挂单")
        if self.notifier:
            self.notifier.notify_pause(self.cfg.symbol_spot)

    def resume(self) -> None:
        """恢复挂单。"""
        with self._state_lock:
            self._paused = False
        logger.info("[CMD] 恢复挂单")
        if self.notifier:
            self.notifier.notify_resume(self.cfg.symbol_spot)

    @property
    def is_paused(self) -> bool:
        with self._state_lock:
            return self._paused

    def set_budget(self, new_budget: float) -> None:
        """运行时修改总预算（币数量）。"""
        from dataclasses import replace
        with self._state_lock:
            old = self.cfg.total_budget
            self.cfg = replace(self.cfg, total_budget=new_budget)
            self.fh.cfg = self.cfg
        logger.info("[CMD] 总预算从 %.4f 币 改为 %.4f 币", old, new_budget)

    def set_min_spread_bps(self, new_bps: float) -> None:
        """运行时修改最小spread门槛（bps）。"""
        from dataclasses import replace
        with self._state_lock:
            old = self.fee.min_spread_bps
            self.fee = replace(self.fee, min_spread_bps=new_bps)
        logger.info("[CMD] 最小spread从 %.4f bps 改为 %.4f bps", old, new_bps)

    @property
    def min_spread(self) -> float:
        return self.fee.min_spread

    @property
    def total_filled_usdt(self) -> float:
        return self._total_filled_usdt

    @property
    def total_filled_base(self) -> float:
        return self._total_filled_base

    @property
    def total_hedged_base(self) -> float:
        return self.fh.total_hedged_base

    @property
    def spot_avg_price(self) -> float | None:
        return self.fh.spot_avg_price

    @property
    def perp_avg_price(self) -> float | None:
        return self.fh.perp_avg_price

    def get_level_weights(self) -> dict[int, float]:
        return dict(self._LEVEL_WEIGHTS)

    def _infer_book_level(self, price: float, spot_bids: list[tuple[float, float]]) -> int | None:
        """根据最新盘口推断订单当前处于买几。"""
        if not spot_bids:
            return None
        tol = max(self.cfg.tick_size_spot * 0.5, 1e-12)
        for i, (bid_price, _) in enumerate(spot_bids, start=1):
            if abs(price - bid_price) <= tol:
                return i
        higher = sum(1 for bid_price, _ in spot_bids if bid_price > price + tol)
        lvl = higher + 1
        if lvl <= len(spot_bids):
            return lvl
        return None

    def get_active_orders_snapshot(self) -> list[dict]:
        with self._state_lock:
            orders = [
                {
                    "id": oid,
                    "level": order.level_idx,
                    "price": order.price,
                    "qty": order.qty,
                    "hedged": order.hedged_qty,
                }
                for oid, order in self._active_orders.items()
            ]
        if not orders:
            return orders

        spot_bids: list[tuple[float, float]] = []
        try:
            spot_bids = self.adapter.get_spot_depth(self.cfg.symbol_spot)
        except Exception:
            logger.exception("[STATUS] 获取盘口失败，返回原始档位")

        for order in orders:
            cur_level = self._infer_book_level(order["price"], spot_bids)
            order["current_level"] = cur_level
        return orders

    def get_status_snapshot(self) -> dict:
        with self._state_lock:
            remaining = self._remaining_budget()
            snap = {
                "paused": self._paused,
                "budget": self.cfg.total_budget,
                "used": round(self._total_filled_base, 6),
                "remaining": round(remaining, 6),
                "used_base": round(self._total_filled_base, 6),
                "spot_filled_base": round(self._total_filled_base, 6),
                "perp_hedged_base": round(self.total_hedged_base, 6),
                "spot_avg_price": (
                    round(self.spot_avg_price, 6)
                    if self.spot_avg_price is not None else None
                ),
                "perp_avg_price": (
                    round(self.perp_avg_price, 6)
                    if self.perp_avg_price is not None else None
                ),
                "perp_avg_priced_base": round(self.fh.total_hedged_base_priced, 6),
                "min_spread_bps": round(self.fee.min_spread_bps, 4),
                "naked_exposure": round(self.naked_exposure, 4),
                "close_task": dict(self._close_task_status),
                "close_pending_hedge": round(self._close_pending_hedge, 6),
            }
        # active_orders 涉及 REST 调用（推断当前档位），在锁外执行避免阻塞主循环
        snap["active_orders"] = self.get_active_orders_snapshot()
        return snap

    def _floor_to_lot(self, qty: float) -> float:
        lot = self.cfg.lot_size
        return int(max(0.0, qty) / lot) * lot

    def start_close_task(self, symbol: str, qty: float) -> tuple[bool, str]:
        """启动平仓任务（异步）。"""
        symbol = symbol.strip().upper()
        if symbol != self.cfg.symbol_spot:
            return False, f"当前仅支持 {self.cfg.symbol_spot}"
        if qty <= 0:
            return False, "数量必须 > 0"

        with self._close_task_lock:
            if self._close_task_running:
                return False, "已有平仓任务在执行"
            self._close_task_running = True
            self._close_task_status = {
                "running": True,
                "symbol": symbol,
                "target_qty": round(qty, 6),
                "spot_sold": 0.0,
                "perp_bought": 0.0,
                "open_orders": [],
                "msg": "平仓任务启动",
            }

        th = threading.Thread(
            target=self._run_close_task,
            args=(symbol, qty),
            daemon=True,
            name="close-task",
        )
        th.start()
        return True, f"已启动平仓任务: {symbol} {qty:.6f} 币"

    def _run_close_task(self, symbol: str, target_qty: float) -> None:
        sold = 0.0
        perp_bought = 0.0
        with self._close_task_lock:
            pending_hedge = max(0.0, self._close_pending_hedge)
        max_rounds = 200
        max_wait_sec = 8.0

        def _format_close_orders(open_orders: dict[str, dict]) -> list[dict]:
            return [
                {
                    "id": oid,
                    "price": info["price"],
                    "qty": info["qty"],
                    "filled": info["filled"],
                }
                for oid, info in open_orders.items()
            ]

        def _update(msg: str, open_orders: dict[str, dict] | None = None) -> None:
            with self._close_task_lock:
                self._close_task_status.update(
                    {
                        "running": True,
                        "spot_sold": round(sold, 6),
                        "perp_bought": round(perp_bought, 6),
                        "pending_hedge": round(pending_hedge, 6),
                        "open_orders": _format_close_orders(open_orders or {}),
                        "msg": msg,
                    }
                )

        try:
            self.pause()
            time.sleep(self.cfg.poll_interval_sec * 2)
            # 主动撤掉所有开仓买单，不依赖主循环
            # 注意：先在锁内拿到需要撤的订单，锁外执行 REST 避免阻塞主循环
            with self._state_lock:
                has_orders = bool(self._active_orders)
            if has_orders:
                logger.info("[CLOSE] 主动撤销开仓买单")
                unhedged = self._cancel_all_orders()
                if unhedged > 1e-12:
                    self.fh.try_hedge(unhedged)
            _update("已暂停开仓并撤销买单，开始执行平仓", {})

            for _ in range(max_rounds):
                remaining = max(0.0, target_qty - sold)
                if remaining < self.cfg.min_order_qty:
                    break

                asks = self.adapter.get_spot_asks(symbol, levels=5)
                fut_ask = self.adapter.get_futures_best_ask(self.cfg.symbol_fut)
                if len(asks) < 3 or fut_ask <= 0:
                    _update("盘口不足，等待下一轮", {})
                    time.sleep(self.cfg.poll_interval_sec)
                    continue

                # 平仓只挂卖二/卖三（避免挂卖一被立即吃掉导致滑点）
                price2 = asks[1][0]
                price3 = asks[2][0]
                spread2 = (price2 - fut_ask) / price2 if price2 > 0 else -1
                spread3 = (price3 - fut_ask) / price3 if price3 > 0 else -1
                if spread2 < self.min_spread or spread3 < self.min_spread:
                    _update("平仓价差未达门槛，等待", {})
                    time.sleep(self.cfg.poll_interval_sec)
                    continue

                qty2 = self._floor_to_lot(remaining * self._LEVEL_WEIGHTS[2])
                qty3 = self._floor_to_lot(remaining - qty2)

                # Notional 下限：确保 price × qty >= 5.5 USDT
                lot = self.cfg.lot_size
                min_notional_qty2 = int(5.5 / price2 / lot + 1) * lot if price2 > 0 else 0
                min_notional_qty3 = int(5.5 / price3 / lot + 1) * lot if price3 > 0 else 0
                if 0 < qty2 < min_notional_qty2:
                    qty2 = min_notional_qty2
                if 0 < qty3 < min_notional_qty3:
                    qty3 = min_notional_qty3

                if qty2 < self.cfg.min_order_qty and qty3 >= self.cfg.min_order_qty:
                    qty2 = 0.0
                if qty3 < self.cfg.min_order_qty and qty2 >= self.cfg.min_order_qty:
                    qty3 = self._floor_to_lot(remaining)
                if qty2 < self.cfg.min_order_qty and qty3 < self.cfg.min_order_qty:
                    break

                open_orders: dict[str, dict] = {}
                if qty2 >= self.cfg.min_order_qty:
                    oid2 = self.adapter.place_spot_limit_sell(symbol, price2, qty2)
                    open_orders[oid2] = {"price": price2, "qty": qty2, "filled": 0.0}
                if qty3 >= self.cfg.min_order_qty:
                    oid3 = self.adapter.place_spot_limit_sell(symbol, price3, qty3)
                    open_orders[oid3] = {"price": price3, "qty": qty3, "filled": 0.0}
                _update(f"已挂平仓卖单 {len(open_orders)} 笔", open_orders)

                started = time.time()
                while open_orders and (time.time() - started) < max_wait_sec:
                    asks_now = self.adapter.get_spot_asks(symbol, levels=5)
                    ask5 = asks_now[4][0] if len(asks_now) >= 5 else None
                    drift = False

                    for oid, info in list(open_orders.items()):
                        filled = self.adapter.get_order_filled_qty(symbol, oid)
                        if filled < 0:
                            continue
                        new_fill = max(0.0, filled - info["filled"])
                        if new_fill > 1e-12:
                            info["filled"] = filled
                            sold += new_fill
                            pending_hedge += new_fill
                            _update("检测到平仓成交，执行永续买入对冲", open_orders)
                            hedge_qty = self._floor_to_lot(pending_hedge)
                            if hedge_qty >= self.cfg.lot_size:
                                try:
                                    self.adapter.place_futures_market_buy(self.cfg.symbol_fut, hedge_qty)
                                    perp_bought += hedge_qty
                                    pending_hedge = max(0.0, pending_hedge - hedge_qty)
                                except Exception:
                                    logger.exception("[CLOSE] 永续买入对冲失败，保留 pending_hedge=%.6f", pending_hedge)
                        if filled >= info["qty"] - 1e-12:
                            open_orders.pop(oid, None)

                        if ask5 is not None and info["price"] > ask5 + self.cfg.tick_size_spot * 0.5:
                            drift = True

                    if drift:
                        _update("卖单已掉出卖五，撤单重挂", open_orders)
                        break
                    time.sleep(0.25)

                for oid, info in list(open_orders.items()):
                    before = self.adapter.get_order_filled_qty(symbol, oid)
                    self.adapter.cancel_order(symbol, oid)
                    after = self.adapter.get_order_filled_qty(symbol, oid)
                    final_filled = info["filled"]
                    if before >= 0:
                        final_filled = max(final_filled, before)
                    if after >= 0:
                        final_filled = max(final_filled, after)
                    new_fill = max(0.0, final_filled - info["filled"])
                    if new_fill > 1e-12:
                        sold += new_fill
                        pending_hedge += new_fill
                    open_orders.pop(oid, None)

                hedge_qty = self._floor_to_lot(pending_hedge)
                if hedge_qty >= self.cfg.lot_size:
                    try:
                        self.adapter.place_futures_market_buy(self.cfg.symbol_fut, hedge_qty)
                        perp_bought += hedge_qty
                        pending_hedge = max(0.0, pending_hedge - hedge_qty)
                    except Exception:
                        logger.exception("[CLOSE] 轮末永续买入对冲失败，保留 pending_hedge=%.6f", pending_hedge)

                _update("本轮平仓完成，检查剩余数量", {})
                time.sleep(self.cfg.poll_interval_sec)

            msg = "平仓完成" if (target_qty - sold) <= self.cfg.min_order_qty else "平仓结束（未完全成交）"
            with self._close_task_lock:
                self._close_pending_hedge = max(0.0, pending_hedge)
                # 将未对冲量转入 naked_exposure，由主循环恢复机制接管
                if pending_hedge > 1e-12:
                    self.fh.naked_exposure += pending_hedge
                    logger.warning("[CLOSE] 平仓结束，pending_hedge=%.6f 转入 naked_exposure", pending_hedge)
                self._close_task_status = {
                    "running": False,
                    "symbol": symbol,
                    "target_qty": round(target_qty, 6),
                    "spot_sold": round(sold, 6),
                    "perp_bought": round(perp_bought, 6),
                    "pending_hedge": round(pending_hedge, 6),
                    "open_orders": [],
                    "msg": msg,
                }
                self._close_task_running = False
        except Exception as exc:
            logger.exception("[CLOSE] 平仓任务失败")
            with self._close_task_lock:
                self._close_pending_hedge = max(0.0, pending_hedge)
                # 将未对冲量转入 naked_exposure，由主循环恢复机制接管
                if pending_hedge > 1e-12:
                    self.fh.naked_exposure += pending_hedge
                    logger.warning("[CLOSE] 平仓异常结束，pending_hedge=%.6f 转入 naked_exposure", pending_hedge)
                self._close_task_status = {
                    "running": False,
                    "symbol": symbol,
                    "target_qty": round(target_qty, 6),
                    "spot_sold": round(sold, 6),
                    "perp_bought": round(perp_bought, 6),
                    "pending_hedge": round(pending_hedge, 6),
                    "open_orders": [],
                    "msg": f"失败: {exc}",
                }
                self._close_task_running = False

    # ── 多档选档 + 深度加权分配 ───────────────────────────────

    # 固定比例分配：买一 20%, 买二 30%, 买三 50%
    _LEVEL_WEIGHTS: dict[int, float] = {1: 0.20, 2: 0.30, 3: 0.50}

    def _select_all_levels(
        self,
        spot_bids: list[tuple[float, float]],
        fut_bid: float,
    ) -> list[tuple[int, float, float]]:
        """选出满足 spread 的档位（买一/买二/买三），按固定比例分配预算。

        当前策略要求买一、买二、买三必须同时满足条件，否则本轮不挂单。
        返回 [(level_idx, price, qty), ...] ，空列表表示本轮不挂。
        """
        if fut_bid <= 0:
            logger.warning("期货 bid 无效: %s", fut_bid)
            return []

        min_spread = self.min_spread
        remaining = self._remaining_budget()
        if remaining <= 0:
            logger.info("[BUDGET] 已买满 %.6f 币 / %.6f 币，停止挂单",
                        self._total_filled_base, self.cfg.total_budget)
            return []
        cycle_budget = min(self.cfg.total_budget * self.cfg.budget_pct, remaining)
        lot = self.cfg.lot_size

        results: list[tuple[int, float, float]] = []
        for level_idx, weight in self._LEVEL_WEIGHTS.items():
            if level_idx > len(spot_bids):
                logger.info("[SELECT] 盘口深度不足，缺少买%d，跳过本轮", level_idx)
                return []
            bid_price, level_qty = spot_bids[level_idx - 1]
            if bid_price <= 0:
                logger.info("[SELECT] 买%d 价格无效，跳过本轮", level_idx)
                return []
            spread = (fut_bid - bid_price) / bid_price
            if spread < min_spread:
                logger.info(
                    "[SELECT] 买%d spread=%.4fbps 低于门槛 %.4fbps，本轮不挂",
                    level_idx, spread * 10000, min_spread * 10000,
                )
                return []

            qty = cycle_budget * weight

            # 深度上限约束
            depth_cap = level_qty * self.cfg.depth_ratio
            qty = min(qty, depth_cap)

            qty = int(qty / lot) * lot

            # Notional 下限：确保 price × qty >= 5.5 USDT（Binance 最低 5）
            min_notional_qty = int(5.5 / bid_price / lot + 1) * lot
            if qty < min_notional_qty:
                qty = min_notional_qty

            if qty < self.cfg.min_order_qty:
                logger.info(
                    "[SELECT] 买%d 数量 %.6f 小于最小下单量 %.6f，本轮不挂",
                    level_idx, qty, self.cfg.min_order_qty,
                )
                return []

            results.append((level_idx, bid_price, qty))
            logger.debug(
                "[SELECT] 买%d: price=%.4f, weight=%d%%, qty=%.2f",
                level_idx, bid_price, int(weight * 100), qty,
            )

        return results

    # ── 单档操作 ──────────────────────────────────────────────

    def _orders_drifted(self, spot_bids: list[tuple[float, float]]) -> bool:
        """检查挂单价格是否已漂移到盘口买5以外（价格上涨导致），需要全撤重挂。"""
        if not self._active_orders or len(spot_bids) < 5:
            return False
        bid5_price = spot_bids[4][0]  # 买五价格
        for order in self._active_orders.values():
            if order.price < bid5_price:
                logger.info(
                    "[DRIFT] 买%d 挂单价 %.4f 已低于盘口买5 %.4f，需全撤重挂",
                    order.level_idx, order.price, bid5_price,
                )
                return True
        return False

    def _orders_below_min_spread(self, fut_bid: float) -> bool:
        """检查当前挂单是否已低于最小利润护栏。"""
        if fut_bid <= 0:
            return False
        for order in self._active_orders.values():
            spread = (fut_bid - order.price) / order.price
            if spread < self.min_spread:
                logger.info(
                    "[GUARD] 买%d 挂单 spread=%.4fbps 低于门槛 %.4fbps，触发撤单",
                    order.level_idx, spread * 10000, self.min_spread * 10000,
                )
                return True
        return False

    def _remaining_budget(self) -> float:
        """剩余可用预算（币数量）。"""
        return max(0.0, self.cfg.total_budget - self._total_filled_base)

    def _need_reprice_level(self, level_idx: int, new_price: float, new_qty: float) -> bool:
        """检查指定档位是否需要改单。价格或数量变化都可触发改单。

        阈值 = max(reprice_bps 计算值, 3 × tick_size)，避免小价格币种频繁改单。
        """
        oid = self._level_to_oid.get(level_idx)
        if oid is None:
            return True
        order = self._active_orders.get(oid)
        if order is None:
            return True
        bps_threshold = order.price * (self.cfg.reprice_bps / 10000.0)
        tick_threshold = self.cfg.tick_size_spot * 3  # 至少变动3个tick才改单
        threshold = max(bps_threshold, tick_threshold)
        price_changed = abs(new_price - order.price) >= threshold
        qty_changed = abs(new_qty - order.qty) >= self.cfg.lot_size / 2
        return price_changed or qty_changed

    def _cancel_level_order(self, level_idx: int) -> float:
        """取消指定档位的挂单，返回发现的未对冲成交量。不清理字典——调用方负责。"""
        oid = self._level_to_oid.get(level_idx)
        if oid is None:
            return 0.0
        order = self._active_orders.get(oid)
        if order is None:
            return 0.0

        # 撤单前检测成交
        unhedged = self.fh.detect_fills_on_cancel(oid, order)

        # 撤单
        self.adapter.cancel_order(self.cfg.symbol_spot, oid)

        # 撤单后再检测一次（捕获撤单瞬间的竞态成交）
        unhedged += self.fh.detect_fills_on_cancel(oid, order)

        return unhedged

    def _cancel_all_orders(self) -> float:
        """取消全部挂单，返回总未对冲成交量。"""
        with self._state_lock:
            levels_to_cancel = list(self._level_to_oid.keys())
        total_unhedged = 0.0
        for level_idx in levels_to_cancel:
            total_unhedged += self._cancel_level_order(level_idx)
        with self._state_lock:
            self._active_orders.clear()
            self._level_to_oid.clear()
        return total_unhedged

    def _place_level_order(self, level_idx: int, price: float, qty: float) -> Optional[str]:
        """在指定档位下现货限价买单。"""
        try:
            oid = self.adapter.place_spot_limit_buy(
                symbol_spot=self.cfg.symbol_spot,
                price=price,
                qty=qty,
            )
            order = LevelOrder(
                level_idx=level_idx,
                order_id=oid,
                price=price,
                qty=qty,
            )
            self._active_orders[oid] = order
            self._level_to_oid[level_idx] = oid

            logger.info("[QUOTE] 买%d 挂单: price=%s, qty=%.2f, order_id=%s",
                        level_idx, price, qty, oid)

            if self.trade_logger:
                self.trade_logger.log_spot_order(self.cfg.symbol_spot, oid, price, qty)
            return oid
        except Exception:
            logger.exception("[QUOTE] 买%d 挂单失败: price=%s, qty=%.2f",
                             level_idx, price, qty)
            return None

    # ── 订单同步（核心） ──────────────────────────────────────

    def _sync_orders(self, desired: list[tuple[int, float, float]]) -> bool:
        """对比目标状态与当前挂单，最小化调整。

        desired: [(level_idx, price, qty), ...]
        返回 True 表示正常；False 表示有风险敞口，暂停开仓。

        注意：不取消 spread 边缘震荡导致暂时不满足条件的档位。
        仅在挂单漂移(drift)时通过主循环的 _orders_drifted 统一撤单。
        """
        desired_map = {d[0]: d for d in desired}
        current_levels = set(self._level_to_oid.keys())
        desired_levels = set(desired_map.keys())

        # 不再取消"不在desired但仍在活跃"的档位 —— 保留它们
        # 只有漂移检查 _orders_drifted 才会全撤
        to_add = desired_levels - current_levels          # 新增档位
        to_check = current_levels & desired_levels        # 可能需要改价

        total_unhedged = 0.0

        # 2. 检查需要改价的档位
        for lv in to_check:
            _, new_price, new_qty = desired_map[lv]
            if self._need_reprice_level(lv, new_price, new_qty):
                total_unhedged += self._cancel_level_order(lv)
                old_oid = self._level_to_oid.pop(lv, None)
                if old_oid:
                    self._active_orders.pop(old_oid, None)
                to_add.add(lv)

        # 3. 先对冲取消过程发现的成交
        if total_unhedged > 1e-12:
            logger.info("[SYNC] 批量对冲撤单发现的成交: qty=%.4f", total_unhedged)
            success, _ = self.fh.try_hedge(total_unhedged)
            if not success:
                remaining = self._cancel_all_orders()
                if remaining > 1e-12:
                    self.fh.try_hedge(remaining)
                return False

        # 4. 有裸露仓位则暂停
        if self.naked_exposure >= self.cfg.lot_size:
            logger.warning("[RISK] 裸露仓位 %.4f，暂停新开仓", self.naked_exposure)
            self._cancel_all_orders()
            return False

        # 5. 下新单
        for lv in sorted(to_add):
            _, price, qty = desired_map[lv]
            self._place_level_order(lv, price, qty)

        return True

    # ── 成交检测 + 对冲（委托 fill_handler.py） ─────────────

    def _check_fills_and_hedge(self) -> None:
        self.fh.check_fills_and_hedge()

    def _try_hedge(self, qty: float) -> tuple[bool, float]:
        return self.fh.try_hedge(qty)

    def _try_recover_naked_exposure(self) -> bool:
        return self.fh.try_recover_naked_exposure()

    # ── 主循环 ────────────────────────────────────────────────

    def run(self) -> None:
        logger.info(
            "启动多档做市套利机器人 | min_spread=%.4fbps, "
            "budget=%.6f 币, 每轮=%.6f 币 (%.1f%%)",
            self.fee.min_spread_bps,
            self.cfg.total_budget,
            self.cfg.total_budget * self.cfg.budget_pct,
            self.cfg.budget_pct * 100,
        )
        while self._running:
            try:
                # 暂停状态：撤单后等待
                if self._paused:
                    if self._active_orders:
                        logger.info("[PAUSE] 暂停中，撤销全部挂单")
                        unhedged = self._cancel_all_orders()
                        if unhedged > 1e-12:
                            self._try_hedge(unhedged)
                    time.sleep(self.cfg.poll_interval_sec)
                    continue

                # 先处理成交对账，避免状态与交易所脱节
                self._check_fills_and_hedge()

                # 优先恢复裸露仓位；若无法恢复则进入保护模式（不新开仓）
                if self.naked_exposure > 0:
                    if not self._try_recover_naked_exposure():
                        if self._active_orders:
                            logger.warning("[RISK] 裸露仓位未恢复，撤销全部挂单并暂停新开仓")
                            unhedged = self._cancel_all_orders()
                            if unhedged > 1e-12:
                                self._try_hedge(unhedged)
                        time.sleep(self.cfg.poll_interval_sec)
                        continue

                # 获取行情
                fut_bid = self.adapter.get_futures_best_bid(self.cfg.symbol_fut)
                spot_bids = self.adapter.get_spot_depth(self.cfg.symbol_spot)

                if not spot_bids:
                    time.sleep(self.cfg.poll_interval_sec)
                    continue

                # 利润护栏：任一活跃挂单低于门槛则先撤再对冲
                if self._active_orders and self._orders_below_min_spread(fut_bid):
                    unhedged = self._cancel_all_orders()
                    if unhedged > 1e-12:
                        self._try_hedge(unhedged)
                    time.sleep(self.cfg.poll_interval_sec)
                    continue

                # 漂移检查：挂单价已低于盘口买5 → 全撤重挂
                if self._orders_drifted(spot_bids):
                    unhedged = self._cancel_all_orders()
                    if unhedged > 1e-12:
                        self._try_hedge(unhedged)
                    # 不 continue，下面会重新选档挂单

                # 买一完全成交 → 先全撤再重挂，避免挂完又撤的浪费
                if self._requote_all_levels:
                    self._requote_all_levels = False
                    unhedged = self._cancel_all_orders()
                    if unhedged > 1e-12:
                        self._try_hedge(unhedged)
                    # 不 continue，直接进入下面的选档重挂

                # 多档选档
                desired = self._select_all_levels(spot_bids, fut_bid)

                if not desired:
                    # 已有挂单时不立即撤销，仅当挂单漂移到买5外时才撤
                    # 避免 spread 在边缘震荡导致反复下撤
                    if not self._active_orders:
                        time.sleep(self.cfg.poll_interval_sec)
                        continue
                    # 有挂单但无合适档位 → 保持现有挂单，等待下次循环
                    time.sleep(self.cfg.poll_interval_sec)
                    continue

                # 同步挂单（最小化调整）
                ok = self._sync_orders(desired)
                if not ok:
                    time.sleep(self.cfg.poll_interval_sec)
                    continue

                # 再做一次成交对账（覆盖本轮下单后的成交事件）
                self._check_fills_and_hedge()

                time.sleep(self.cfg.poll_interval_sec)

            except KeyboardInterrupt:
                logger.info("收到键盘中断，退出...")
                break
            except Exception:
                logger.exception("主循环异常，%.1f秒后重试", self.cfg.poll_interval_sec * 5)
                time.sleep(self.cfg.poll_interval_sec * 5)

        logger.info("套利机器人已停止")
