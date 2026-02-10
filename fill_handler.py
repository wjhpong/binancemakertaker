"""成交检测 + 对冲模块 —— 从 arbitrage_bot.py 拆出。

后期切实盘 WebSocket 接口时，只需修改本文件。
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from contextlib import nullcontext
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from arbitrage_bot import ExchangeAdapter, LevelOrder, StrategyConfig

logger = logging.getLogger(__name__)


class FillHandler:
    """负责成交检测、记账、合约对冲。

    bot 持有本对象，调用 check_fills_and_hedge() / try_hedge() 等方法。
    """

    def __init__(
        self,
        adapter: ExchangeAdapter,
        cfg: StrategyConfig,
        active_orders: dict[str, LevelOrder],
        state_lock: threading.RLock | None = None,
        fill_queue: Optional[queue.Queue] = None,
        trade_logger=None,
        notifier=None,
    ) -> None:
        self.adapter = adapter
        self.cfg = cfg
        # 与 bot 共享同一个 dict 引用
        self._active_orders = active_orders
        self._state_lock = state_lock
        self.fill_queue = fill_queue
        self.trade_logger = trade_logger
        self.notifier = notifier

        self._hedge_lock = threading.Lock()
        self.naked_exposure: float = 0.0
        self.total_filled_usdt: float = 0.0
        self.total_filled_base: float = 0.0
        self.total_hedged_base: float = 0.0
        self.total_hedged_base_priced: float = 0.0
        self.total_hedged_quote: float = 0.0
        self._last_rest_reconcile_ts: float = 0.0
        self._last_gap_check_ts: float = 0.0

    def _state_guard(self):
        return self._state_lock if self._state_lock is not None else nullcontext()

    def reset_counters(self) -> None:
        """清零所有统计计数器（开启新一轮时调用）。"""
        self.total_filled_base = 0.0
        self.total_filled_usdt = 0.0
        self.total_hedged_base = 0.0
        self.total_hedged_base_priced = 0.0
        self.total_hedged_quote = 0.0
        self.naked_exposure = 0.0

    # ── WS 成交事件消费 ──────────────────────────────────────

    def drain_fill_queue(self) -> dict[str, float]:
        """从 WS fill_queue 读取所有活跃订单的成交事件。

        返回 {order_id: 最新累计成交量}。
        """
        if self.fill_queue is None:
            return {}
        fills: dict[str, float] = {}
        while True:
            try:
                event = self.fill_queue.get_nowait()
            except queue.Empty:
                break
            oid = event.get("order_id")
            with self._state_guard():
                order = self._active_orders.get(oid)
            if order is not None:
                fills[oid] = event["filled_qty"]
                logger.info(
                    "[WS_FILL] 买%d order_id=%s 累计=%s, 本次=%s @ %s",
                    order.level_idx, oid, event["filled_qty"],
                    event["last_filled_qty"], event["last_filled_price"],
                )
            else:
                logger.debug("[WS_FILL] 忽略非活跃订单: %s", oid)
        return fills

    # ── 成交记账 ─────────────────────────────────────────────

    def record_spot_fill(self, order: LevelOrder, cum_filled: float) -> float:
        """将新增现货成交记入预算与台账，返回新增成交量。"""
        with self._state_guard():
            new_fill = cum_filled - order.accounted_qty
            if new_fill <= 1e-12:
                return 0.0
            self.total_filled_base += new_fill
            self.total_filled_usdt += new_fill * order.price
            order.accounted_qty = cum_filled
        if self.trade_logger:
            self.trade_logger.log_spot_fill(
                self.cfg.symbol_spot, order.order_id, order.price, new_fill
            )
        return new_fill

    # ── 撤单时的成交检测 ─────────────────────────────────────

    def detect_fills_on_cancel(self, oid: str, order: LevelOrder) -> float:
        """撤单前/后检测该订单的成交量，返回未对冲成交量。

        注意：会同步更新 order.hedged_qty，避免被多次调用时重复计算。
        """
        unhedged = 0.0
        ws_fills = self.drain_fill_queue()
        if oid in ws_fills:
            self.record_spot_fill(order, ws_fills[oid])
            with self._state_guard():
                new_fill = ws_fills[oid] - order.hedged_qty
                if new_fill > 1e-12:
                    unhedged += new_fill
                    order.hedged_qty += new_fill
        else:
            filled = self.adapter.get_order_filled_qty(self.cfg.symbol_spot, oid)
            if filled >= 0:
                self.record_spot_fill(order, filled)
                with self._state_guard():
                    new_fill = filled - order.hedged_qty
                    if new_fill > 1e-12:
                        unhedged += new_fill
                        order.hedged_qty += new_fill
        return unhedged

    # ── 批量成交检测 + 对冲 ──────────────────────────────────

    def check_fills_and_hedge(self) -> None:
        """检查所有活跃订单成交情况，批量对冲。

        优先使用 WS fill_queue（零 REST 开销），REST 仅作兜底对账。
        """
        with self._state_guard():
            if not self._active_orders:
                return
            active_order_ids = list(self._active_orders.keys())
        if not active_order_ids:
            return

        now = time.time()
        # 优先 WS fill_queue
        if self.fill_queue is not None:
            order_fills = self.drain_fill_queue()
            # 兜底对账：定期用 REST 校验，避免 WS 丢包导致漏对冲
            if now - self._last_rest_reconcile_ts >= 10.0:
                for oid in active_order_ids:
                    filled = self.adapter.get_order_filled_qty(self.cfg.symbol_spot, oid)
                    if filled >= 0:
                        order_fills[oid] = max(order_fills.get(oid, 0.0), filled)
                self._last_rest_reconcile_ts = now
        else:
            # 无 WS 时回退 REST
            order_fills = {}
            for oid in active_order_ids:
                filled = self.adapter.get_order_filled_qty(self.cfg.symbol_spot, oid)
                if filled >= 0:
                    order_fills[oid] = filled

        # 汇总新增成交（锁内仅做内存状态更新，避免把日志/IO 放在临界区）
        total_new_unhedged = 0.0
        per_order_unhedged: dict[str, float] = {}
        fully_filled: list[str] = []
        trade_logs: list[tuple[str, str, float, float]] = []

        with self._state_guard():
            for oid, cum_filled in order_fills.items():
                order = self._active_orders.get(oid)
                if order is None:
                    continue
                new_fill = cum_filled - order.accounted_qty
                if new_fill > 1e-12:
                    self.total_filled_base += new_fill
                    self.total_filled_usdt += new_fill * order.price
                    order.accounted_qty = cum_filled
                    trade_logs.append((self.cfg.symbol_spot, order.order_id, order.price, new_fill))
                unhedged = cum_filled - order.hedged_qty
                if unhedged > 1e-12:
                    per_order_unhedged[oid] = unhedged
                    total_new_unhedged += unhedged
                if cum_filled >= order.qty - 1e-12:
                    fully_filled.append(oid)

        if self.trade_logger:
            for symbol, order_id, price, qty in trade_logs:
                self.trade_logger.log_spot_fill(symbol, order_id, price, qty)

        # 批量对冲
        if total_new_unhedged > 1e-12:
            _, hedged_qty = self.try_hedge(total_new_unhedged)
            remaining_hedge = hedged_qty
            # 在一次锁内完成对冲量分配，保证 hedged_qty 更新的原子性
            with self._state_guard():
                order_levels = {
                    oid: self._active_orders[oid].level_idx
                    for oid in per_order_unhedged.keys()
                    if oid in self._active_orders
                }
                for oid in sorted(per_order_unhedged.keys(), key=lambda x: order_levels.get(x, 999)):
                    order = self._active_orders.get(oid)
                    if order is None:
                        continue
                    unhedged = per_order_unhedged[oid]
                    if remaining_hedge <= 1e-12:
                        break
                    hedged_part = min(unhedged, remaining_hedge)
                    order.hedged_qty += hedged_part
                    remaining_hedge -= hedged_part
            logger.info("[PROGRESS] 累计成交: %.6f / %.6f 币",
                        self.total_filled_base, self.cfg.total_budget)

        # 清理完全成交的订单（需要 bot 的 _level_to_oid，通过回调通知）
        for oid in fully_filled:
            with self._state_guard():
                order = self._active_orders.get(oid)
            if order and order.hedged_qty >= order.qty - 1e-12:
                logger.info(
                    "买%d 完全成交: order_id=%s, price=%.4f, qty=%.2f",
                    order.level_idx, oid, order.price, order.qty,
                )
                if self._on_order_fully_filled:
                    self._on_order_fully_filled(oid, order)

        # ── 定期差额对账：确保 filled - hedged 不会无限漂移 ──
        now2 = time.time()
        if now2 - self._last_gap_check_ts >= 30.0:
            self._last_gap_check_ts = now2
            need_hedge_gap = False
            with self._state_guard():
                gap = self.total_filled_base - self.total_hedged_base - self.naked_exposure
                if gap >= self.cfg.lot_size:
                    self.naked_exposure += gap
                    need_hedge_gap = True
            if gap >= self.cfg.lot_size:
                logger.warning(
                    "[GAP] 发现对冲缺口: filled=%.4f, hedged=%.4f, naked=%.4f, gap=%.4f → 补充对冲",
                    self.total_filled_base, self.total_hedged_base,
                    self.naked_exposure, gap,
                )
                if need_hedge_gap:
                    self.try_hedge(0)  # qty=0，靠 naked_exposure 驱动对冲
            elif gap < -self.cfg.lot_size:
                logger.warning(
                    "[GAP] 发现超额对冲: filled=%.4f, hedged=%.4f, naked=%.4f, gap=%.4f（已多对冲，记录告警）",
                    self.total_filled_base, self.total_hedged_base,
                    self.naked_exposure, gap,
                )

    _on_order_fully_filled = None  # 回调：bot 设置，用于清理 _level_to_oid

    @property
    def spot_avg_price(self) -> float | None:
        if self.total_filled_base <= 1e-12:
            return None
        return self.total_filled_usdt / self.total_filled_base

    @property
    def perp_avg_price(self) -> float | None:
        if self.total_hedged_base_priced <= 1e-12 or self.total_hedged_quote <= 1e-12:
            return None
        return self.total_hedged_quote / self.total_hedged_base_priced

    # ── 对冲执行 ─────────────────────────────────────────────

    def try_hedge(self, qty: float) -> tuple[bool, float]:
        """合约市价卖出对冲，带重试。返回 (成功, 实际对冲量)。"""
        with self._hedge_lock:
            with self._state_guard():
                current_naked = self.naked_exposure
                total_filled_snapshot = self.total_filled_base
                lot = self.cfg.lot_size
                total_to_hedge = max(0.0, qty + current_naked)
                hedge_qty = int(total_to_hedge / lot) * lot
                residual = max(0.0, total_to_hedge - hedge_qty)

            if qty <= 0 and total_to_hedge <= 0:
                return True, 0.0
            if hedge_qty < lot:
                with self._state_guard():
                    self.naked_exposure = total_to_hedge
                logger.info(
                    "[HEDGE] 待对冲累计 %.8f 小于 lot_size=%s，继续累计等待",
                    total_to_hedge, lot
                )
                return True, 0.0

            for i in range(self.cfg.max_retry):
                try:
                    hedge_id = self.adapter.place_futures_market_sell(self.cfg.symbol_fut, hedge_qty)
                    hedge_price = getattr(self.adapter, "last_hedge_avg_price", None)
                    logger.info("[HEDGE] 合约对冲成功: qty=%s, order_id=%s, price=%s",
                                hedge_qty, hedge_id, hedge_price)
                    with self._state_guard():
                        self.total_hedged_base += hedge_qty
                        if hedge_price and hedge_price > 0:
                            self.total_hedged_quote += hedge_qty * hedge_price
                            self.total_hedged_base_priced += hedge_qty
                        self.naked_exposure = residual
                    if self.trade_logger:
                        self.trade_logger.log_hedge(
                            self.cfg.symbol_fut, hedge_id, hedge_qty, success=True, price=hedge_price
                        )
                    if self.notifier:
                        self.notifier.notify_open_trade(
                            symbol=self.cfg.symbol_spot,
                            hedge_qty=hedge_qty,
                            hedge_price=hedge_price,
                            total_filled=total_filled_snapshot,
                            total_budget=self.cfg.total_budget,
                        )
                    if residual > 1e-12:
                        logger.warning(
                            "[HEDGE] 对冲后残余 %.8f 未覆盖，计入裸露仓位",
                            residual
                        )
                        return False, hedge_qty
                    return True, hedge_qty
                except Exception as exc:
                    if self._is_notional_too_small(exc):
                        logger.warning(
                            "[HEDGE] 对冲量 %s 名义价值不足合约最小下单额(5 USDT)，"
                            "转入裸露仓位等待累计",
                            hedge_qty,
                        )
                        with self._state_guard():
                            self.naked_exposure = total_to_hedge
                        return False, 0.0
                    logger.warning("[HEDGE] 重试 %d/%d 失败: %s", i + 1, self.cfg.max_retry, exc)
                    if i + 1 < self.cfg.max_retry:
                        time.sleep(0.15)

            logger.critical("[HEDGE] 对冲彻底失败! qty=%s 转入裸露仓位", hedge_qty)
            with self._state_guard():
                self.naked_exposure = total_to_hedge
            if self.trade_logger:
                self.trade_logger.log_hedge(self.cfg.symbol_fut, "", hedge_qty, success=False)
            return False, 0.0

    # ── 裸露仓位恢复 ─────────────────────────────────────────

    @staticmethod
    def _is_notional_too_small(exc: Exception) -> bool:
        """检查异常是否为合约最小名义价值不足（错误码 -4164）。"""
        args = getattr(exc, "args", ())
        if args and len(args) >= 2 and args[1] == -4164:
            return True
        return "-4164" in str(exc)

    def try_recover_naked_exposure(self) -> bool:
        """尝试对冲裸露仓位，返回是否恢复成功。"""
        with self._hedge_lock:
            with self._state_guard():
                naked = self.naked_exposure
                total_filled_snapshot = self.total_filled_base
                lot = self.cfg.lot_size

            if naked <= 0:
                return True
            if naked < lot:
                logger.info(
                    "[RECOVER] 裸露仓位 %.8f 小于 lot_size=%s，等待后续成交累计",
                    naked, lot,
                )
                return True
            logger.warning("[RECOVER] 尝试恢复裸露仓位: %s", naked)
            for i in range(self.cfg.max_retry):
                try:
                    hedge_qty = int(naked / lot) * lot
                    if hedge_qty < lot:
                        logger.warning(
                            "[RECOVER] 裸露仓位 %.8f 小于 lot_size=%s，无法自动恢复，保持停机保护",
                            naked, lot,
                        )
                        return False
                    hedge_id = self.adapter.place_futures_market_sell(self.cfg.symbol_fut, hedge_qty)
                    hedge_price = getattr(self.adapter, "last_hedge_avg_price", None)
                    logger.info("[RECOVER] 裸露仓位已对冲: qty=%s, order_id=%s, price=%s",
                                hedge_qty, hedge_id, hedge_price)
                    with self._state_guard():
                        self.total_hedged_base += hedge_qty
                        if hedge_price and hedge_price > 0:
                            self.total_hedged_quote += hedge_qty * hedge_price
                            self.total_hedged_base_priced += hedge_qty
                        self.naked_exposure = max(0.0, self.naked_exposure - hedge_qty)
                    if self.trade_logger:
                        self.trade_logger.log_hedge(
                            self.cfg.symbol_fut, hedge_id, hedge_qty,
                            success=True, price=hedge_price,
                        )
                    if self.notifier:
                        self.notifier.notify_open_trade(
                            symbol=self.cfg.symbol_spot,
                            hedge_qty=hedge_qty,
                            hedge_price=hedge_price,
                            total_filled=total_filled_snapshot,
                            total_budget=self.cfg.total_budget,
                        )
                    return True
                except Exception as exc:
                    if self._is_notional_too_small(exc):
                        logger.warning(
                            "[RECOVER] 裸露仓位 %.4f 名义价值不足合约最小下单额(5 USDT)，"
                            "放弃对冲并清零（损失可忽略）",
                            naked,
                        )
                        with self._state_guard():
                            self.naked_exposure = 0.0
                        return True
                    logger.warning("[RECOVER] 重试 %d/%d 失败: %s", i + 1, self.cfg.max_retry, exc)
                    if i + 1 < self.cfg.max_retry:
                        time.sleep(0.15)
            logger.critical("[RECOVER] 裸露仓位恢复失败，等待下一轮重试")
            return False
