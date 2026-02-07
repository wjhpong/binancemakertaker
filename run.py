#!/usr/bin/env python3
"""启动入口 —— 加载配置、初始化组件、信号处理、优雅退出。
远程控制通过 ctl.py + Unix socket 实现。
"""

from __future__ import annotations

import logging
import os
import signal
import sys
from dataclasses import replace

from arbitrage_bot import SpotFuturesArbitrageBot
from binance_adapter import BinanceAdapter
from config import load_config
from control_server import ControlServer
from feishu_notifier import FeishuNotifier
from trade_logger import TradeLogger
from ws_manager import WSManager

logger = logging.getLogger("run")

_FEISHU_WEBHOOK = os.environ.get(
    "FEISHU_WEBHOOK",
    "",
)


def main() -> None:
    # ── 预初始化日志（确保 config 加载阶段也有日志输出）──
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.StreamHandler()],
    )

    # ── 命令行参数：可选覆盖总预算 ──
    budget_override = None
    if len(sys.argv) > 1:
        try:
            budget_override = float(sys.argv[1])
            logger.info("命令行指定总预算: %.6f 币", budget_override)
        except ValueError:
            print("用法: python run.py [总预算币数量]  例如: python run.py 8000")
            sys.exit(1)

    # ── 加载配置 ──
    api_key, api_secret, testnet, fee, cfg, log_config = load_config()

    # 命令行覆盖预算
    if budget_override is not None:
        cfg = replace(cfg, total_budget=budget_override)

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
    logger.info("挂单范围: 买%d ~ 买%d | budget=%.6f 币, 单笔<=%s%%预算, <=%s%%档深",
                cfg.min_level, cfg.max_level, cfg.total_budget,
                cfg.budget_pct * 100, cfg.depth_ratio * 100)
    logger.info("=" * 60)

    # ── 初始化飞书通知器 ──
    notifier = None
    if _FEISHU_WEBHOOK:
        notifier = FeishuNotifier(_FEISHU_WEBHOOK)
        logger.info("飞书通知已启用")

    # ── 初始化组件 ──
    trade_log = TradeLogger()

    ws = WSManager(
        symbol=cfg.symbol_spot,
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
    bot.notifier = notifier

    # 飞书启动通知
    if notifier:
        level_desc = ", ".join(
            f"买{k}={int(v * 100)}%" for k, v in bot.get_level_weights().items()
        )
        notifier.notify_start(cfg.symbol_spot, cfg.total_budget, level_desc, testnet)

    # ── 信号处理 ──
    def shutdown(signum, _frame):
        sig_name = signal.Signals(signum).name
        logger.info("收到信号 %s，准备退出...", sig_name)
        bot.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ── 控制服务器（Unix socket，供 ctl.py 远程控制） ──
    ctrl = ControlServer(bot)
    ctrl.start()

    # ── 运行 ──
    try:
        bot.run()
    finally:
        ctrl.stop()
        # 清理：撤销所有残留挂单
        for order in bot.get_active_orders_snapshot():
            try:
                adapter.cancel_order(cfg.symbol_spot, order["id"])
                logger.info("已撤销残留挂单: 买%d order_id=%s", order["level"], order["id"])
            except Exception:
                logger.exception("退出时撤单失败: order_id=%s", order["id"])

        # 警告裸露仓位
        if bot.naked_exposure > 0:
            logger.critical("!!! 退出时存在裸露仓位: %s — 请手动处理 !!!", bot.naked_exposure)

        # 飞书停止通知
        if notifier:
            notifier.notify_stop(bot.total_filled_base, cfg.total_budget, bot.naked_exposure)

        ws.stop()
        trade_log.close()
        logger.info("套利机器人已完全退出")


if __name__ == "__main__":
    main()
