#!/usr/bin/env python3
"""启动入口 —— 加载配置、初始化组件、信号处理、优雅退出。"""

from __future__ import annotations

import logging
import signal
from dataclasses import replace

from arbitrage_bot import SpotFuturesArbitrageBot
from binance_adapter import BinanceAdapter
from config import load_config
from trade_logger import TradeLogger
from ws_manager import WSManager

logger = logging.getLogger("run")


def main() -> None:
    # ── 预初始化日志（确保 config 加载阶段也有日志输出）──
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.StreamHandler()],
    )

    # ── 加载配置 ──
    api_key, api_secret, testnet, fee, cfg, log_config = load_config()

    # ── 根据配置重新设定日志 ──
    root = logging.getLogger()
    root.setLevel(getattr(logging, log_config["level"].upper(), logging.INFO))
    file_handler = logging.FileHandler(log_config["file"], encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.addHandler(file_handler)

    logger.info("=" * 60)
    logger.info("同所做市套利机器人启动")
    logger.info("testnet=%s | symbol_spot=%s | symbol_fut=%s",
                testnet, cfg.symbol_spot, cfg.symbol_fut)
    logger.info("maker返佣=%.4f%% | taker费=%.4f%% | 净成本=%.4f%% | 最小利润=%sbps",
                fee.spot_maker * 100, fee.fut_taker * 100,
                fee.net_cost * 100, fee.min_profit_bps)
    logger.info("挂单范围: 买%d ~ 买%d | budget=%sU, 单笔<=%s%%金额, <=%s%%档深",
                cfg.min_level, cfg.max_level, cfg.total_budget,
                cfg.budget_pct * 100, cfg.depth_ratio * 100)
    logger.info("=" * 60)

    # ── 初始化组件 ──
    trade_log = TradeLogger()

    ws = WSManager(
        symbol=cfg.symbol_spot,  # Binance spot/futures 都是 BTCUSDT
        testnet=testnet,
        api_key=api_key,
    )
    ws.start()

    adapter = BinanceAdapter(
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        price_cache=ws.price_cache,
    )

    # 启动前校验交易对
    exchange_info = adapter.preflight_check(cfg.symbol_spot, cfg.symbol_fut)
    if exchange_info:
        # 检查 config 里的 tick_size 和交易所返回的是否一致
        real_tick = exchange_info.get("spot_tick_size")
        if real_tick and abs(real_tick - cfg.tick_size_spot) > 1e-12:
            logger.warning(
                "config.yaml 中 tick_size_spot=%.8f 与交易所实际值 %.8f 不一致，建议修正",
                cfg.tick_size_spot, real_tick,
            )
        spot_lot = exchange_info.get("spot_lot_size")
        fut_lot = exchange_info.get("fut_lot_size")
        lot_candidates = [x for x in (cfg.lot_size, spot_lot, fut_lot) if x and x > 0]
        if lot_candidates:
            effective_lot = max(lot_candidates)
            if abs(effective_lot - cfg.lot_size) > 1e-12:
                logger.warning(
                    "lot_size 从 %.8f 自动提升到 %.8f，确保现货/合约两边都可下单",
                    cfg.lot_size, effective_lot,
                )
                cfg = replace(cfg, lot_size=effective_lot)

    bot = SpotFuturesArbitrageBot(
        adapter=adapter,
        fee=fee,
        cfg=cfg,
        trade_logger=trade_log,
        fill_queue=ws.fill_queue,
    )

    # ── 信号处理 ──
    def shutdown(signum, _frame):
        sig_name = signal.Signals(signum).name
        logger.info("收到信号 %s，准备退出...", sig_name)
        bot.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ── 运行 ──
    try:
        bot.run()
    finally:
        # 清理：撤销所有残留挂单
        for oid, order in list(bot._active_orders.items()):
            try:
                adapter.cancel_order(cfg.symbol_spot, oid)
                logger.info("已撤销残留挂单: 买%d order_id=%s", order.level_idx, oid)
            except Exception:
                logger.exception("退出时撤单失败: order_id=%s", oid)

        # 警告裸露仓位
        if bot.naked_exposure > 0:
            logger.critical("!!! 退出时存在裸露仓位: %s — 请手动处理 !!!", bot.naked_exposure)

        ws.stop()
        trade_log.close()
        logger.info("套利机器人已完全退出")


if __name__ == "__main__":
    main()
