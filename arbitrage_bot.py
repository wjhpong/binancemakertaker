#!/usr/bin/env python3
"""
同所期现套利开单逻辑（多档现货挂买单 -> 成交后合约市价卖出对冲）
"""

from __future__ import annotations

import logging
import queue
import time
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FeeConfig:
    spot_maker: float = 0.000675    # VIP2 maker 0.0675%
    fut_taker: float = 0.00036     # VIP2 taker 0.036%
    min_profit_bps: float = 0.5    # 最小利润要求 (bps)

    @property
    def net_cost(self) -> float:
        return self.fut_taker + self.spot_maker

    @property
    def min_spread(self) -> float:
        return self.net_cost + self.min_profit_bps / 10000.0


@dataclass(frozen=True)
class StrategyConfig:
    symbol_spot: str
    symbol_fut: str
    tick_size_spot: float
    total_budget: float             # 总买入金额 (USDT)
    budget_pct: float = 0.01        # 每轮总预算不超过总金额的 1%
    depth_ratio: float = 0.3        # 单档不超过该档深度的 30%
    min_order_qty: float = 0.00001
    lot_size: float = 0.00001
    min_level: int = 2
    max_level: int = 5
    poll_interval_sec: float = 0.2
    reprice_bps: float = 0.5
    max_retry: int = 3


class ExchangeAdapter:
    def get_futures_best_bid(self, symbol_fut: str) -> float:
        raise NotImplementedError

    def get_spot_depth(self, symbol_spot: str, levels: int = 5) -> list[tuple[float, float]]:
        raise NotImplementedError

    def get_spot_open_bid_order(self, symbol_spot: str) -> Optional[dict]:
        raise NotImplementedError

    def place_spot_limit_buy(self, symbol_spot: str, price: float, qty: float) -> str:
        raise NotImplementedError

    def cancel_order(self, symbol: str, order_id: str) -> None:
        raise NotImplementedError

    def get_order_filled_qty(self, symbol: str, order_id: str) -> float:
        raise NotImplementedError

    def place_futures_market_sell(self, symbol_fut: str, qty: float) -> str:
        raise NotImplementedError


@dataclass
class LevelOrder:
    """跟踪单个档位的现货限价买单。"""
    level_idx: int       # 2..5 (买二..买五)
    order_id: str
    price: float
    qty: float
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
        self.adapter = adapter
        self.fee = fee
        self.cfg = cfg
        self.trade_logger = trade_logger
        self.fill_queue = fill_queue

        # 多档挂单状态
        self._active_orders: dict[str, LevelOrder] = {}   # order_id → LevelOrder
        self._level_to_oid: dict[int, str] = {}            # level_idx → order_id

        self.naked_exposure: float = 0.0
        self._total_filled_usdt: float = 0.0  # 累计已成交金额(USDT)
        self._running: bool = True
        self._paused: bool = False

        # 飞书通知器（可选）
        self.notifier = None

    def stop(self) -> None:
        self._running = False

    def pause(self) -> None:
        """暂停挂单（撤销所有挂单，但不退出主循环）。"""
        self._paused = True
        logger.info("[CMD] 暂停挂单")

    def resume(self) -> None:
        """恢复挂单。"""
        self._paused = False
        logger.info("[CMD] 恢复挂单")

    @property
    def is_paused(self) -> bool:
        return self._paused

    def set_budget(self, new_budget: float) -> None:
        """运行时修改总预算。"""
        from dataclasses import replace
        old = self.cfg.total_budget
        self.cfg = replace(self.cfg, total_budget=new_budget)
        logger.info("[CMD] 总预算从 %.0fU 改为 %.0fU", old, new_budget)

    # ── 多档选档 + 深度加权分配 ───────────────────────────────

    # 固定比例分配：买二 30%, 买三 70%
    _LEVEL_WEIGHTS: dict[int, float] = {2: 0.30, 3: 0.70}

    def _select_all_levels(
        self,
        spot_bids: list[tuple[float, float]],
        fut_bid: float,
    ) -> list[tuple[int, float, float]]:
        """选出满足 spread 的档位（买二/买三），按固定比例分配预算。

        返回 [(level_idx, price, qty), ...] ，空列表表示无合适档位。
        """
        if fut_bid <= 0:
            logger.warning("期货 bid 无效: %s", fut_bid)
            return []

        min_spread = self.fee.min_spread
        remaining = self._remaining_budget()
        if remaining < 1.0:
            logger.info("[BUDGET] 已买满 %.2fU / %.2fU，停止挂单",
                        self._total_filled_usdt, self.cfg.total_budget)
            if self.notifier:
                self.notifier.notify_budget_complete(self.cfg.total_budget)
            return []
        cycle_budget = min(self.cfg.total_budget * self.cfg.budget_pct, remaining)
        lot = self.cfg.lot_size

        results: list[tuple[int, float, float]] = []
        for level_idx, weight in self._LEVEL_WEIGHTS.items():
            if level_idx > len(spot_bids):
                break
            bid_price, level_qty = spot_bids[level_idx - 1]
            if bid_price <= 0:
                continue
            spread = (fut_bid - bid_price) / bid_price
            if spread < min_spread:
                continue

            qty = (cycle_budget * weight) / bid_price

            # 深度上限约束
            depth_cap = level_qty * self.cfg.depth_ratio
            qty = min(qty, depth_cap)

            qty = int(qty / lot) * lot
            if qty < self.cfg.min_order_qty:
                continue

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

    def _remaining_budget(self) -> float:
        """剩余可用预算(USDT)。"""
        return max(0.0, self.cfg.total_budget - self._total_filled_usdt)

    def _need_reprice_level(self, level_idx: int, new_price: float, new_qty: float) -> bool:
        """检查指定档位是否需要改单。只在价格变化时触发，深度波动不触发改单。

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
        return abs(new_price - order.price) >= threshold

    def _cancel_level_order(self, level_idx: int) -> float:
        """取消指定档位的挂单，返回发现的未对冲成交量。不清理字典——调用方负责。

        通过 WS fill_queue 检测成交（避免 REST 查单消耗 rate limit）。
        """
        oid = self._level_to_oid.get(level_idx)
        if oid is None:
            return 0.0
        order = self._active_orders.get(oid)
        if order is None:
            return 0.0

        # 先消化 fill_queue 里该订单的成交事件
        unhedged = 0.0
        ws_fills = self._drain_fill_queue()
        if oid in ws_fills:
            new_fill = ws_fills[oid] - order.hedged_qty
            if new_fill > 1e-12:
                unhedged += new_fill
                order.hedged_qty = ws_fills[oid]

        # 撤单
        self.adapter.cancel_order(self.cfg.symbol_spot, oid)

        # 撤单后再消化一次 fill_queue（捕获撤单瞬间的竞态成交）
        ws_fills = self._drain_fill_queue()
        if oid in ws_fills:
            new_fill = ws_fills[oid] - order.hedged_qty
            if new_fill > 1e-12:
                unhedged += new_fill

        return unhedged

    def _cancel_all_orders(self) -> float:
        """取消全部挂单，返回总未对冲成交量。"""
        total_unhedged = 0.0
        for level_idx in list(self._level_to_oid.keys()):
            total_unhedged += self._cancel_level_order(level_idx)
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
        """
        desired_map = {d[0]: d for d in desired}
        current_levels = set(self._level_to_oid.keys())
        desired_levels = set(desired_map.keys())

        to_cancel = current_levels - desired_levels      # 不再需要的档位
        to_add = desired_levels - current_levels          # 新增档位
        to_check = current_levels & desired_levels        # 可能需要改价

        total_unhedged = 0.0

        # 1. 取消不再需要的档位
        for lv in to_cancel:
            total_unhedged += self._cancel_level_order(lv)
            oid = self._level_to_oid.pop(lv, None)
            if oid:
                self._active_orders.pop(oid, None)

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
            success = self._try_hedge(total_unhedged)
            if not success:
                remaining = self._cancel_all_orders()
                if remaining > 1e-12:
                    self._try_hedge(remaining)
                return False

        # 4. 有裸露仓位则暂停
        if self.naked_exposure > 0:
            logger.warning("[RISK] 裸露仓位 %.4f，暂停新开仓", self.naked_exposure)
            self._cancel_all_orders()
            return False

        # 5. 下新单
        for lv in sorted(to_add):
            _, price, qty = desired_map[lv]
            self._place_level_order(lv, price, qty)

        return True

    # ── 成交检测 + 批量对冲 ───────────────────────────────────

    def _drain_fill_queue(self) -> dict[str, float]:
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

    def _check_fills_and_hedge(self) -> None:
        """检查所有活跃订单成交情况，批量对冲。

        优先使用 WS fill_queue（零 REST 开销），REST 仅在无 fill_queue 时使用。
        """
        if not self._active_orders:
            return

        # 优先 WS fill_queue
        if self.fill_queue is not None:
            order_fills = self._drain_fill_queue()
        else:
            # 无 WS 时回退 REST
            order_fills = {}
            for oid in list(self._active_orders.keys()):
                filled = self.adapter.get_order_filled_qty(self.cfg.symbol_spot, oid)
                if filled >= 0:
                    order_fills[oid] = filled

        # 汇总新增成交
        total_new_fill = 0.0
        fully_filled: list[str] = []

        for oid, cum_filled in order_fills.items():
            order = self._active_orders.get(oid)
            if order is None:
                continue
            new_fill = cum_filled - order.hedged_qty
            if new_fill > 1e-12:
                total_new_fill += new_fill

            if cum_filled >= order.qty - 1e-12:
                fully_filled.append(oid)

        # 批量对冲
        if total_new_fill > 1e-12:
            success = self._try_hedge(total_new_fill)
            if success:
                for oid, cum_filled in order_fills.items():
                    order = self._active_orders.get(oid)
                    if order is None:
                        continue
                    new_fill = cum_filled - order.hedged_qty
                    if new_fill > 1e-12:
                        # 累计成交金额
                        self._total_filled_usdt += new_fill * order.price
                        if self.trade_logger:
                            self.trade_logger.log_spot_fill(
                                self.cfg.symbol_spot, oid, order.price, new_fill,
                            )
                    order.hedged_qty = cum_filled
                    # 飞书通知
                    if self.notifier and new_fill > 1e-12:
                        self.notifier.notify_fill(
                            symbol=self.cfg.symbol_spot,
                            level_idx=order.level_idx,
                            price=order.price,
                            qty=new_fill,
                            filled_usdt=new_fill * order.price,
                            total_filled_usdt=self._total_filled_usdt,
                            total_budget=self.cfg.total_budget,
                        )
                logger.info("[PROGRESS] 累计成交: %.2fU / %.2fU",
                            self._total_filled_usdt, self.cfg.total_budget)

        # 清理完全成交的订单
        for oid in fully_filled:
            order = self._active_orders.get(oid)
            if order and order.hedged_qty >= order.qty - 1e-12:
                logger.info(
                    "买%d 完全成交: order_id=%s, price=%.4f, qty=%.2f",
                    order.level_idx, oid, order.price, order.qty,
                )
                self._level_to_oid.pop(order.level_idx, None)
                self._active_orders.pop(oid, None)

    # ── 对冲 ──────────────────────────────────────────────────

    def _try_hedge(self, qty: float) -> bool:
        if qty <= 0:
            return True
        # 按合约 lot_size 取整
        lot = self.cfg.lot_size
        hedge_qty = int(qty / lot) * lot
        if hedge_qty < lot:
            logger.debug("[HEDGE] 累积量 %.4f < lot_size=%s，暂不对冲", qty, lot)
            return True

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
                    self.notifier.notify_hedge(
                        self.cfg.symbol_fut, hedge_qty, hedge_price, success=True,
                    )
                return True
            except Exception as exc:
                logger.warning("[HEDGE] 重试 %d/%d 失败: %s", i + 1, self.cfg.max_retry, exc)
                if i + 1 < self.cfg.max_retry:
                    time.sleep(0.15)

        logger.critical("[HEDGE] 对冲彻底失败! qty=%s 转入裸露仓位", hedge_qty)
        self.naked_exposure += hedge_qty
        if self.trade_logger:
            self.trade_logger.log_hedge(self.cfg.symbol_fut, "", hedge_qty, success=False)
        if self.notifier:
            self.notifier.notify_hedge(self.cfg.symbol_fut, hedge_qty, None, success=False)
        return False

    def _try_recover_naked_exposure(self) -> bool:
        if self.naked_exposure <= 0:
            return True
        logger.warning("[RECOVER] 尝试恢复裸露仓位: %s", self.naked_exposure)
        for i in range(self.cfg.max_retry):
            try:
                hedge_id = self.adapter.place_futures_market_sell(
                    self.cfg.symbol_fut, self.naked_exposure
                )
                hedge_price = getattr(self.adapter, "last_hedge_avg_price", None)
                logger.info("[RECOVER] 裸露仓位已对冲: qty=%s, order_id=%s, price=%s",
                            self.naked_exposure, hedge_id, hedge_price)
                if self.trade_logger:
                    self.trade_logger.log_hedge(
                        self.cfg.symbol_fut, hedge_id, self.naked_exposure,
                        success=True, price=hedge_price,
                    )
                self.naked_exposure = 0.0
                return True
            except Exception as exc:
                logger.warning("[RECOVER] 重试 %d/%d 失败: %s", i + 1, self.cfg.max_retry, exc)
                if i + 1 < self.cfg.max_retry:
                    time.sleep(0.15)
        logger.critical("[RECOVER] 裸露仓位恢复失败，等待下一轮重试")
        return False

    # ── 主循环 ────────────────────────────────────────────────

    def run(self) -> None:
        logger.info(
            "启动多档做市套利机器人 | net_cost=%.4f%%, min_spread=%.4f%%, "
            "budget=%.0fU, 每轮=%.0fU (%.1f%%)",
            self.fee.net_cost * 100,
            self.fee.min_spread * 100,
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

                # 优先恢复裸露仓位
                if self.naked_exposure > 0:
                    if not self._try_recover_naked_exposure():
                        time.sleep(self.cfg.poll_interval_sec)
                        continue

                # 获取行情
                fut_bid = self.adapter.get_futures_best_bid(self.cfg.symbol_fut)
                spot_bids = self.adapter.get_spot_depth(self.cfg.symbol_spot)

                if not spot_bids:
                    time.sleep(self.cfg.poll_interval_sec)
                    continue

                # 漂移检查：挂单价已低于盘口买5 → 全撤重挂
                if self._orders_drifted(spot_bids):
                    unhedged = self._cancel_all_orders()
                    if unhedged > 1e-12:
                        self._try_hedge(unhedged)
                    # 不 continue，下面会重新选档挂单

                # 多档选档
                desired = self._select_all_levels(spot_bids, fut_bid)

                if not desired:
                    if self._active_orders:
                        logger.info("[NO_LEVEL] 无合适档位，撤销全部挂单")
                        unhedged = self._cancel_all_orders()
                        if unhedged > 1e-12:
                            self._try_hedge(unhedged)
                    time.sleep(self.cfg.poll_interval_sec)
                    continue

                # 同步挂单（最小化调整）
                ok = self._sync_orders(desired)
                if not ok:
                    time.sleep(self.cfg.poll_interval_sec)
                    continue

                # 检查成交 + 批量对冲
                self._check_fills_and_hedge()

                time.sleep(self.cfg.poll_interval_sec)

            except KeyboardInterrupt:
                logger.info("收到键盘中断，退出...")
                break
            except Exception:
                logger.exception("主循环异常，%.1f秒后重试", self.cfg.poll_interval_sec * 5)
                time.sleep(self.cfg.poll_interval_sec * 5)

        logger.info("套利机器人已停止")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    fee = FeeConfig()
    cfg = StrategyConfig(
        symbol_spot="ASTERUSDT",
        symbol_fut="ASTERUSDT",
        tick_size_spot=0.001,
        total_budget=4000.0,
        min_level=2,
        max_level=5,
    )

    adapter = ExchangeAdapter()
    bot = SpotFuturesArbitrageBot(adapter=adapter, fee=fee, cfg=cfg)
    bot.run()


if __name__ == "__main__":
    main()
