#!/usr/bin/env python3
"""
同所期现套利开单逻辑（现货挂买单 -> 成交后合约市价卖出对冲）

你需要把 ExchangeAdapter 里的 TODO 接到真实交易所 API。
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
    spot_maker: float = -0.00025    # maker 返佣（负值表示返佣, BNB 抵扣后 -0.025%）
    fut_taker: float = 0.0004      # 合约 taker 费率 0.04%
    min_profit_bps: float = 0.5    # 最小利润要求 (bps), 0.5bp

    @property
    def net_cost(self) -> float:
        """净成本 = taker费 - maker返佣（返佣为负，所以实际是加法）。
        例: 0.0004 + (-0.00025) = 0.00015 = 1.5bp
        """
        return self.fut_taker + self.spot_maker

    @property
    def min_spread(self) -> float:
        """最小要求价差比例 = 净成本 + 最小利润。"""
        return self.net_cost + self.min_profit_bps / 10000.0


@dataclass(frozen=True)
class StrategyConfig:
    symbol_spot: str
    symbol_fut: str
    tick_size_spot: float
    total_budget: float             # 总买入金额 (USDT)
    budget_pct: float = 0.01        # 单笔不超过总金额的 1%
    depth_ratio: float = 0.3        # 单笔不超过该档深度的 30%
    min_order_qty: float = 0.00001  # 最小下单量 (BTC)
    lot_size: float = 0.00001       # 交易所最小精度 (stepSize)
    min_level: int = 2              # 最浅挂单档位（1=买一, 2=买二...）
    max_level: int = 5              # 最深挂单档位
    poll_interval_sec: float = 0.2
    reprice_bps: float = 0.5        # 触发改单阈值（bps）
    max_retry: int = 3


class ExchangeAdapter:
    """
    交易所接口抽象层。
    把下面方法替换成你的 SDK/REST/WebSocket 逻辑。
    """

    def get_futures_best_bid(self, symbol_fut: str) -> float:
        raise NotImplementedError("TODO: 接入合约盘口 bid")

    def get_spot_depth(self, symbol_spot: str, levels: int = 5) -> list[tuple[float, float]]:
        """返回现货买盘深度 [(price, qty), ...]，价格从高到低。"""
        raise NotImplementedError("TODO: 接入现货深度")

    def get_spot_open_bid_order(self, symbol_spot: str) -> Optional[dict]:
        raise NotImplementedError("TODO: 查询现货当前挂单（买单）")

    def place_spot_limit_buy(self, symbol_spot: str, price: float, qty: float) -> str:
        raise NotImplementedError("TODO: 下现货限价买单，返回 order_id")

    def cancel_order(self, symbol: str, order_id: str) -> None:
        raise NotImplementedError("TODO: 撤单")

    def get_order_filled_qty(self, symbol: str, order_id: str) -> float:
        raise NotImplementedError("TODO: 获取订单累计成交量")

    def place_futures_market_sell(self, symbol_fut: str, qty: float) -> str:
        raise NotImplementedError("TODO: 合约市价卖出对冲")


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
        self.fill_queue = fill_queue  # WS 用户数据流成交推送队列
        self.current_spot_order_id: Optional[str] = None
        self.last_quote_price: Optional[float] = None
        self._current_order_qty: float = 0.0   # 当前挂单数量（动态计算）
        self.hedged_qty: float = 0.0           # 当前订单已对冲的数量
        self.naked_exposure: float = 0.0       # 对冲失败的裸露仓位
        self._running: bool = True

    def stop(self) -> None:
        """由信号处理器调用，通知主循环退出。"""
        self._running = False

    def _drain_fill_queue(self) -> float:
        """从 WS fill_queue 中读取当前订单的累计成交量。

        返回累计成交量（来自 WS 推送的 filled_qty 字段）。
        如果没有 fill_queue 或无相关事件，返回 -1 表示无数据。
        """
        if self.fill_queue is None:
            return -1.0
        latest_filled = -1.0
        while True:
            try:
                event = self.fill_queue.get_nowait()
            except queue.Empty:
                break
            # 只处理当前订单的成交事件
            if (self.current_spot_order_id and
                    event.get("order_id") == self.current_spot_order_id):
                latest_filled = event["filled_qty"]  # 累计成交量
                logger.info("[WS_FILL] order_id=%s 累计成交=%s, 本次=%s @ %s",
                            event["order_id"], event["filled_qty"],
                            event["last_filled_qty"], event["last_filled_price"])
            else:
                logger.debug("[WS_FILL] 忽略非当前订单事件: %s", event.get("order_id"))
        return latest_filled

    # ── 动态数量 ──────────────────────────────────────────────

    def _calc_order_qty(self, bid_price: float, level_qty: float) -> float:
        """根据约束计算本次挂单数量。

        约束:
          1. 不超过总金额的 budget_pct (默认 1%)
          2. 不超过该档深度的 depth_ratio (默认 30%)
          3. 不低于 min_order_qty
        """
        # 约束1: 总金额的 1% → 换算成 BTC 数量
        budget_qty = (self.cfg.total_budget * self.cfg.budget_pct) / bid_price
        # 约束2: 该档深度的 30%
        depth_qty = level_qty * self.cfg.depth_ratio
        # 取两者较小值
        qty = min(budget_qty, depth_qty)
        # 按 lot_size 向下取整
        lot = self.cfg.lot_size
        qty = int(qty / lot) * lot
        return qty

    # ── 选档逻辑 ──────────────────────────────────────────────

    def _select_level(
        self,
        spot_bids: list[tuple[float, float]],
        fut_bid: float,
    ) -> Optional[tuple[float, int, float]]:
        """从 min_level 到 max_level 遍历现货买盘，选择 spread 能覆盖成本的最浅档。

        返回 (挂单价格, 档位索引, 挂单数量) 或 None（无合适档位）。
        档位索引从 1 开始：1=买一, 2=买二 ...
        """
        if fut_bid <= 0:
            logger.warning("期货 bid 无效: %s", fut_bid)
            return None

        min_spread = self.fee.min_spread

        for level_idx in range(self.cfg.min_level, self.cfg.max_level + 1):
            if level_idx > len(spot_bids):
                break
            bid_price, level_qty = spot_bids[level_idx - 1]  # 0-indexed
            if bid_price <= 0:
                continue
            # spread = (fut_bid - spot_bid) / spot_bid
            spread = (fut_bid - bid_price) / bid_price
            if spread >= min_spread:
                qty = self._calc_order_qty(bid_price, level_qty)
                if qty < self.cfg.min_order_qty:
                    logger.debug(
                        "[SELECT] 买%d spread 满足但深度不足: qty=%.8f < min=%.8f",
                        level_idx, qty, self.cfg.min_order_qty,
                    )
                    continue
                logger.debug(
                    "[SELECT] 选中买%d: spot_bid=%.2f, fut_bid=%.2f, "
                    "spread=%.4f%%, qty=%.8f",
                    level_idx, bid_price, fut_bid, spread * 100, qty,
                )
                return (bid_price, level_idx, qty)

        return None

    # ── 改单判断 ──────────────────────────────────────────────

    def _need_reprice(self, new_price: float, new_qty: float) -> bool:
        if self.last_quote_price is None:
            return True
        threshold = self.last_quote_price * (self.cfg.reprice_bps / 10000.0)
        price_changed = abs(new_price - self.last_quote_price) >= threshold
        qty_changed = abs(new_qty - self._current_order_qty) >= self.cfg.lot_size / 2
        return price_changed or qty_changed

    # ── 挂单 / 撤单 ──────────────────────────────────────────

    def _hedge_remaining_before_cancel(self) -> bool:
        """撤单前检查是否有未对冲的成交量，有则先对冲。"""
        if not self.current_spot_order_id:
            return True
        try:
            filled = self.adapter.get_order_filled_qty(
                self.cfg.symbol_spot, self.current_spot_order_id
            )
            # 哨兵值 -1 表示订单查不到，跳过对冲检查
            if filled < 0:
                logger.warning("撤单前查单返回哨兵值，跳过对冲检查")
                return True
            new_fill = filled - self.hedged_qty
            if new_fill > 1e-12:
                logger.info("撤单前发现未对冲成交: qty=%s", new_fill)
                success = self._try_hedge(new_fill)
                if success:
                    self.hedged_qty = filled
                else:
                    return False
            return True
        except Exception:
            logger.exception("撤单前检查成交量失败")
            return False

    def _cancel_and_replace_spot_order(self, new_price: float, qty: float) -> bool:
        # 撤单前先对冲已成交但未对冲的部分
        hedge_ok = self._hedge_remaining_before_cancel()

        if self.current_spot_order_id:
            self.adapter.cancel_order(self.cfg.symbol_spot, self.current_spot_order_id)
            # 撤单后再查一次，防止撤单瞬间成交的竞态
            try:
                filled = self.adapter.get_order_filled_qty(
                    self.cfg.symbol_spot, self.current_spot_order_id
                )
                # 哨兵值 -1 表示订单查不到，跳过
                if filled >= 0:
                    new_fill = filled - self.hedged_qty
                    if new_fill > 1e-12:
                        logger.info("撤单后发现最后成交: qty=%s", new_fill)
                        success = self._try_hedge(new_fill)
                        if success:
                            self.hedged_qty = filled
                        else:
                            hedge_ok = False
                else:
                    logger.warning("撤单后查单返回哨兵值，跳过竞态检查")
            except Exception:
                logger.exception("撤单后检查成交量失败")
                hedge_ok = False
            self.current_spot_order_id = None

        if not hedge_ok or self.naked_exposure > 0:
            logger.warning("[RISK] 检测到未恢复风险敞口，暂停新开仓")
            self.last_quote_price = None
            self._current_order_qty = 0.0
            self.hedged_qty = 0.0
            return False

        # 重置状态，下新单
        self.hedged_qty = 0.0
        self._current_order_qty = qty
        self.current_spot_order_id = self.adapter.place_spot_limit_buy(
            symbol_spot=self.cfg.symbol_spot,
            price=new_price,
            qty=qty,
        )
        self.last_quote_price = new_price
        logger.info("[QUOTE] 现货买单更新: price=%s, qty=%.8f", new_price, qty)

        if self.trade_logger:
            self.trade_logger.log_spot_order(
                self.cfg.symbol_spot, self.current_spot_order_id, new_price, qty
            )
        return True

    # ── 对冲 ──────────────────────────────────────────────────

    def _try_hedge(self, qty: float) -> bool:
        """尝试合约市价卖出对冲。成功返回 True，失败累加到 naked_exposure。"""
        if qty <= 0:
            return True
        for i in range(self.cfg.max_retry):
            try:
                hedge_id = self.adapter.place_futures_market_sell(self.cfg.symbol_fut, qty)
                # 尝试读取成交均价（BinanceAdapter 会缓存）
                hedge_price = getattr(self.adapter, "last_hedge_avg_price", None)
                logger.info("[HEDGE] 合约对冲成功: qty=%s, order_id=%s, price=%s",
                            qty, hedge_id, hedge_price)
                if self.trade_logger:
                    self.trade_logger.log_hedge(
                        self.cfg.symbol_fut, hedge_id, qty, success=True, price=hedge_price
                    )
                return True
            except Exception as exc:
                logger.warning(
                    "[HEDGE] 重试 %d/%d 失败: %s", i + 1, self.cfg.max_retry, exc
                )
                if i + 1 < self.cfg.max_retry:
                    time.sleep(0.15)

        logger.critical("[HEDGE] 对冲彻底失败! qty=%s 转入裸露仓位", qty)
        self.naked_exposure += qty
        if self.trade_logger:
            self.trade_logger.log_hedge(self.cfg.symbol_fut, "", qty, success=False)
        return False

    def _try_recover_naked_exposure(self) -> bool:
        """尝试对冲裸露仓位。成功返回 True。"""
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
            "启动做市套利机器人 | net_cost=%.4f%%, min_spread=%.4f%%, levels=%d~%d",
            self.fee.net_cost * 100,
            self.fee.min_spread * 100,
            self.cfg.min_level,
            self.cfg.max_level,
        )
        while self._running:
            try:
                # 优先处理裸露仓位，恢复前不开新单
                if self.naked_exposure > 0:
                    if not self._try_recover_naked_exposure():
                        time.sleep(self.cfg.poll_interval_sec)
                        continue

                # 获取合约买一 + 现货多档深度
                fut_bid = self.adapter.get_futures_best_bid(self.cfg.symbol_fut)
                spot_bids = self.adapter.get_spot_depth(self.cfg.symbol_spot)

                if not spot_bids:
                    time.sleep(self.cfg.poll_interval_sec)
                    continue

                # 动态选档
                result = self._select_level(spot_bids, fut_bid)

                if result is None:
                    # 无合适档位 —— 如果有挂单就撤掉，但先对冲已成交部分保持平衡
                    if self.current_spot_order_id:
                        logger.info("[NO_LEVEL] 无合适档位，撤销现有挂单")
                        hedge_ok = self._hedge_remaining_before_cancel()
                        self.adapter.cancel_order(self.cfg.symbol_spot, self.current_spot_order_id)
                        # 撤单后再查一次，防止撤单瞬间成交的竞态
                        try:
                            filled = self.adapter.get_order_filled_qty(
                                self.cfg.symbol_spot, self.current_spot_order_id
                            )
                            # 哨兵值 -1 表示订单查不到，跳过
                            if filled >= 0:
                                new_fill = filled - self.hedged_qty
                                if new_fill > 1e-12:
                                    logger.info("[NO_LEVEL] 撤单后发现竞态成交: qty=%s", new_fill)
                                    success = self._try_hedge(new_fill)
                                    if success:
                                        self.hedged_qty = filled
                                    else:
                                        hedge_ok = False
                            else:
                                logger.warning("[NO_LEVEL] 撤单后查单返回哨兵值，跳过竞态检查")
                        except Exception:
                            logger.exception("[NO_LEVEL] 撤单后检查成交量失败")
                            hedge_ok = False
                        if not hedge_ok or self.naked_exposure > 0:
                            logger.warning("[RISK] 撤单后存在未恢复风险敞口，暂停新开仓")
                        self.current_spot_order_id = None
                        self.last_quote_price = None
                        self._current_order_qty = 0.0
                        self.hedged_qty = 0.0
                    time.sleep(self.cfg.poll_interval_sec)
                    continue

                target_price, level, order_qty = result

                # 需要改单？
                if self._need_reprice(target_price, order_qty):
                    logger.info("[REPRICE] 目标买%d: price=%.2f, qty=%.8f, fut_bid=%.2f",
                                level, target_price, order_qty, fut_bid)
                    placed = self._cancel_and_replace_spot_order(target_price, order_qty)
                    if not placed:
                        time.sleep(self.cfg.poll_interval_sec)
                        continue

                # 检查成交：优先 REST，REST 失败时回退 WS fill_queue
                if self.current_spot_order_id:
                    filled = self.adapter.get_order_filled_qty(
                        self.cfg.symbol_spot, self.current_spot_order_id
                    )
                    # REST 返回哨兵值 -1 → 回退到 WS fill_queue
                    if filled < 0:
                        filled = self._drain_fill_queue()
                    else:
                        # REST 成功时也清空 fill_queue 防止堆积
                        self._drain_fill_queue()

                    if filled >= 0:
                        new_fill = filled - self.hedged_qty
                        if new_fill > 1e-12:
                            success = self._try_hedge(new_fill)
                            if success:
                                if self.trade_logger and self.last_quote_price is not None:
                                    self.trade_logger.log_spot_fill(
                                        self.cfg.symbol_spot,
                                        self.current_spot_order_id,
                                        self.last_quote_price,
                                        new_fill,
                                    )
                                self.hedged_qty = filled
                            # 全部成交 → 重置状态
                            if success and filled >= self._current_order_qty - 1e-12:
                                spread_bps = ((fut_bid - target_price) / target_price) * 10000
                                logger.info(
                                    "套利完成: 买%d @ %.2f, qty=%.8f, 合约 @ %.2f, spread=%.2fbps",
                                    level, target_price, self._current_order_qty, fut_bid, spread_bps,
                                )
                                self.current_spot_order_id = None
                                self.last_quote_price = None
                                self._current_order_qty = 0.0
                                self.hedged_qty = 0.0

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
    fee = FeeConfig(
        spot_maker=-0.00025,
        fut_taker=0.0004,
        min_profit_bps=0.5,
    )
    cfg = StrategyConfig(
        symbol_spot="BTCUSDT",
        symbol_fut="BTCUSDT",
        tick_size_spot=0.01,
        total_budget=10000.0,
        min_level=2,
        max_level=5,
    )

    adapter = ExchangeAdapter()
    bot = SpotFuturesArbitrageBot(adapter=adapter, fee=fee, cfg=cfg)
    bot.run()


if __name__ == "__main__":
    main()
