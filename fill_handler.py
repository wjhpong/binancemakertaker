"""成交检测 + 对冲模块 —— 从 arbitrage_bot.py 拆出。

后期切实盘 WebSocket 接口时，只需修改本文件。
"""

from __future__ import annotations

import logging
import queue
import time
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
        fill_queue: Optional[queue.Queue] = None,
        trade_logger=None,
        notifier=None,
    ) -> None:
        self.adapter = adapter
        self.cfg = cfg
        # 与 bot 共享同一个 dict 引用
        self._active_orders = active_orders
        self.fill_queue = fill_queue
        self.trade_logger = trade_logger
        self.notifier = notifier

        self.naked_exposure: float = 0.0
        self.total_filled_usdt: float = 0.0
        self.total_filled_base: float = 0.0
        self.total_hedged_base: float = 0.0
        self._last_rest_reconcile_ts: float = 0.0

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
            if oid in self._active_orders:
                fills[oid] = event["filled_qty"]
                order = self._active_orders[oid]
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
        new_fill = cum_filled - order.accounted_qty
        if new_fill <= 1e-12:
            return 0.0
        self.total_filled_base += new_fill
        self.total_filled_usdt += new_fill * order.price
        if self.trade_logger:
            self.trade_logger.log_spot_fill(
                self.cfg.symbol_spot, order.order_id, order.price, new_fill
            )
        order.accounted_qty = cum_filled
        return new_fill

    # ── 撤单时的成交检测 ─────────────────────────────────────

    def detect_fills_on_cancel(self, oid: str, order: LevelOrder) -> float:
        """撤单前/后检测该订单的成交量，返回未对冲成交量。"""
        unhedged = 0.0
        ws_fills = self.drain_fill_queue()
        if oid in ws_fills:
            self.record_spot_fill(order, ws_fills[oid])
            new_fill = ws_fills[oid] - order.hedged_qty
            if new_fill > 1e-12:
                unhedged += new_fill
        else:
            filled = self.adapter.get_order_filled_qty(self.cfg.symbol_spot, oid)
            if filled >= 0:
                self.record_spot_fill(order, filled)
                new_fill = filled - order.hedged_qty
                if new_fill > 1e-12:
                    unhedged += new_fill
        return unhedged

    # ── 批量成交检测 + 对冲 ──────────────────────────────────

    def check_fills_and_hedge(self) -> None:
        """检查所有活跃订单成交情况，批量对冲。

        优先使用 WS fill_queue（零 REST 开销），REST 仅作兜底对账。
        """
        if not self._active_orders:
            return

        now = time.time()
        # 优先 WS fill_queue
        if self.fill_queue is not None:
            order_fills = self.drain_fill_queue()
            # 兜底对账：定期用 REST 校验，避免 WS 丢包导致漏对冲
            if now - self._last_rest_reconcile_ts >= 2.0:
                for oid in list(self._active_orders.keys()):
                    filled = self.adapter.get_order_filled_qty(self.cfg.symbol_spot, oid)
                    if filled >= 0:
                        order_fills[oid] = max(order_fills.get(oid, 0.0), filled)
                self._last_rest_reconcile_ts = now
        else:
            # 无 WS 时回退 REST
            order_fills = {}
            for oid in list(self._active_orders.keys()):
                filled = self.adapter.get_order_filled_qty(self.cfg.symbol_spot, oid)
                if filled >= 0:
                    order_fills[oid] = filled

        # 汇总新增成交
        total_new_unhedged = 0.0
        per_order_unhedged: dict[str, float] = {}
        fully_filled: list[str] = []

        for oid, cum_filled in order_fills.items():
            order = self._active_orders.get(oid)
            if order is None:
                continue
            self.record_spot_fill(order, cum_filled)
            unhedged = cum_filled - order.hedged_qty
            if unhedged > 1e-12:
                per_order_unhedged[oid] = unhedged
                total_new_unhedged += unhedged

            if cum_filled >= order.qty - 1e-12:
                fully_filled.append(oid)

        # 批量对冲
        if total_new_unhedged > 1e-12:
            _, hedged_qty = self.try_hedge(total_new_unhedged)
            remaining_hedge = hedged_qty
            for oid in sorted(
                per_order_unhedged.keys(),
                key=lambda x: self._active_orders[x].level_idx,
            ):
                order = self._active_orders.get(oid)
                if order is None:
                    continue
                unhedged = per_order_unhedged[oid]
                if remaining_hedge <= 1e-12:
                    break
                hedged_part = min(unhedged, remaining_hedge)
                order.hedged_qty += hedged_part
                remaining_hedge -= hedged_part
            logger.info("[PROGRESS] 累计成交: %.2fU / %.2fU",
                        self.total_filled_usdt, self.cfg.total_budget)

        # 清理完全成交的订单（需要 bot 的 _level_to_oid，通过回调通知）
        for oid in fully_filled:
            order = self._active_orders.get(oid)
            if order and order.hedged_qty >= order.qty - 1e-12:
                logger.info(
                    "买%d 完全成交: order_id=%s, price=%.4f, qty=%.2f",
                    order.level_idx, oid, order.price, order.qty,
                )
                if self._on_order_fully_filled:
                    self._on_order_fully_filled(oid, order)

    _on_order_fully_filled = None  # 回调：bot 设置，用于清理 _level_to_oid

    # ── 对冲执行 ─────────────────────────────────────────────

    def try_hedge(self, qty: float) -> tuple[bool, float]:
        """合约市价卖出对冲，带重试。返回 (成功, 实际对冲量)。"""
        if qty <= 0:
            return True, 0.0
        lot = self.cfg.lot_size
        hedge_qty = int(qty / lot) * lot
        residual = max(0.0, qty - hedge_qty)
        if hedge_qty < lot:
            self.naked_exposure += qty
            logger.warning(
                "[HEDGE] 需对冲量 %.8f 小于 lot_size=%s，计入裸露仓位",
                qty, lot
            )
            return False, 0.0

        for i in range(self.cfg.max_retry):
            try:
                hedge_id = self.adapter.place_futures_market_sell(self.cfg.symbol_fut, hedge_qty)
                hedge_price = getattr(self.adapter, "last_hedge_avg_price", None)
                logger.info("[HEDGE] 合约对冲成功: qty=%s, order_id=%s, price=%s",
                            hedge_qty, hedge_id, hedge_price)
                if self.trade_logger:
                    self.trade_logger.log_hedge(
                        self.cfg.symbol_fut, hedge_id, hedge_qty, success=True, price=hedge_price
                    )
                if self.notifier:
                    self.notifier.notify_progress(
                        symbol=self.cfg.symbol_spot,
                        total_spot_filled_base=self.total_filled_base,
                        total_perp_hedged_base=self.total_hedged_base + hedge_qty,
                    )
                self.total_hedged_base += hedge_qty
                if residual > 1e-12:
                    self.naked_exposure += residual
                    logger.warning(
                        "[HEDGE] 对冲后残余 %.8f 未覆盖，计入裸露仓位",
                        residual
                    )
                    return False, hedge_qty
                return True, hedge_qty
            except Exception as exc:
                logger.warning("[HEDGE] 重试 %d/%d 失败: %s", i + 1, self.cfg.max_retry, exc)
                if i + 1 < self.cfg.max_retry:
                    time.sleep(0.15)

        logger.critical("[HEDGE] 对冲彻底失败! qty=%s 转入裸露仓位", hedge_qty)
        self.naked_exposure += qty
        if self.trade_logger:
            self.trade_logger.log_hedge(self.cfg.symbol_fut, "", hedge_qty, success=False)
        return False, 0.0

    # ── 裸露仓位恢复 ─────────────────────────────────────────

    def try_recover_naked_exposure(self) -> bool:
        """尝试对冲裸露仓位，返回是否恢复成功。"""
        if self.naked_exposure <= 0:
            return True
        logger.warning("[RECOVER] 尝试恢复裸露仓位: %s", self.naked_exposure)
        for i in range(self.cfg.max_retry):
            try:
                lot = self.cfg.lot_size
                hedge_qty = int(self.naked_exposure / lot) * lot
                if hedge_qty < lot:
                    logger.warning(
                        "[RECOVER] 裸露仓位 %.8f 小于 lot_size=%s，无法自动恢复，保持停机保护",
                        self.naked_exposure, lot,
                    )
                    return False
                hedge_id = self.adapter.place_futures_market_sell(self.cfg.symbol_fut, hedge_qty)
                hedge_price = getattr(self.adapter, "last_hedge_avg_price", None)
                logger.info("[RECOVER] 裸露仓位已对冲: qty=%s, order_id=%s, price=%s",
                            hedge_qty, hedge_id, hedge_price)
                if self.trade_logger:
                    self.trade_logger.log_hedge(
                        self.cfg.symbol_fut, hedge_id, hedge_qty,
                        success=True, price=hedge_price,
                    )
                self.total_hedged_base += hedge_qty
                self.naked_exposure = max(0.0, self.naked_exposure - hedge_qty)
                if self.notifier:
                    self.notifier.notify_progress(
                        symbol=self.cfg.symbol_spot,
                        total_spot_filled_base=self.total_filled_base,
                        total_perp_hedged_base=self.total_hedged_base,
                    )
                return True
            except Exception as exc:
                logger.warning("[RECOVER] 重试 %d/%d 失败: %s", i + 1, self.cfg.max_retry, exc)
                if i + 1 < self.cfg.max_retry:
                    time.sleep(0.15)
        logger.critical("[RECOVER] 裸露仓位恢复失败，等待下一轮重试")
        return False
