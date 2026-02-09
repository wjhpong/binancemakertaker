#!/usr/bin/env python3
"""启动入口 —— 加载配置、初始化组件、信号处理、优雅退出。
远程控制通过 ctl.py + Unix socket 实现。
"""

from __future__ import annotations

import logging
import signal
import sys
from dataclasses import replace

from arbitrage_bot import SpotFuturesArbitrageBot
from binance_adapter import BinanceAdapter
from config import ConfigError, load_config
from control_server import ControlServer
from feishu_notifier import FeishuNotifier
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
    try:
        api_key, api_secret, fee, cfg, log_config = load_config()
    except ConfigError as exc:
        logger.error("配置加载失败: %s", exc)
        sys.exit(1)

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
    logger.info("symbol_spot=%s | symbol_fut=%s",
                cfg.symbol_spot, cfg.symbol_fut)
    logger.info("maker费=%.4f%% | taker费=%.4f%% | 最小spread=%.4fbps",
                fee.spot_maker * 100, fee.fut_taker * 100,
                fee.min_spread_bps)
    logger.info("挂单范围: 买1~买3 | budget=%.6f 币, 单笔<=%s%%预算, <=%s%%档深",
                cfg.total_budget, cfg.budget_pct * 100, cfg.depth_ratio * 100)
    logger.info("=" * 60)

    # ── 初始化飞书通知器 ──
    # 注意：必须在 load_config() 之后读取，确保 .env 已加载进环境变量
    import os
    feishu_webhook = os.environ.get("FEISHU_WEBHOOK", "")
    notifier = None
    if feishu_webhook:
        notifier = FeishuNotifier(feishu_webhook)
        logger.info("飞书通知已启用")

    # ── 初始化组件 ──
    trade_log = TradeLogger()

    ws = WSManager(
        symbol=cfg.symbol_spot,
        api_key=api_key,
    )
    ws.start()

    adapter = BinanceAdapter(
        api_key=api_key,
        api_secret=api_secret,
        price_cache=ws.price_cache,
    )

    # 启动前校验交易对
    exchange_info = adapter.preflight_check(cfg.symbol_spot, cfg.symbol_fut)
    if exchange_info:
        real_tick = exchange_info.get("spot_tick_size")
        if real_tick and abs(real_tick - cfg.tick_size_spot) > 1e-12:
            logger.warning(
                "tick_size_spot 从 %.8f 自动修正为交易所实际值 %.8f",
                cfg.tick_size_spot, real_tick,
            )
            cfg = replace(cfg, tick_size_spot=real_tick)
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
    if notifier:
        notifier.notify_start(cfg.symbol_spot)

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

        ws.stop()
        trade_log.close()
        logger.info("套利机器人已完全退出")


if __name__ == "__main__":
    main()
