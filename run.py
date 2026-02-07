#!/usr/bin/env python3
"""启动入口 —— 加载配置、初始化组件、交互式命令行、优雅退出。"""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
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
    "https://open.feishu.cn/open-apis/bot/v2/hook/da41fa3f-a538-40a8-a226-4b9de2e3e083",
)


def _print_help() -> None:
    print(
        "\n"
        "===== 命令行控制 =====\n"
        "  start       开始挂单\n"
        "  pause       暂停挂单（撤销全部挂单）\n"
        "  stop        停止并退出\n"
        "  budget N    设置总预算为 N USDT（如 budget 8000）\n"
        "  status      查看当前状态\n"
        "  help        显示此帮助\n"
        "========================\n"
    )


def _cmd_loop(bot: SpotFuturesArbitrageBot) -> None:
    """在独立线程中运行交互式命令行。"""
    _print_help()
    while bot._running:
        try:
            line = input("> ").strip().lower()
        except EOFError:
            break
        except KeyboardInterrupt:
            bot.stop()
            break

        if not line:
            continue

        parts = line.split()
        cmd = parts[0]

        if cmd == "start":
            if bot.is_paused:
                bot.resume()
                print("已恢复挂单")
            else:
                print("已在运行中")

        elif cmd == "pause":
            if not bot.is_paused:
                bot.pause()
                print("已暂停，全部挂单将撤销")
            else:
                print("已经是暂停状态")

        elif cmd == "stop":
            print("正在停止...")
            bot.stop()
            break

        elif cmd == "budget":
            if len(parts) < 2:
                print(f"当前预算: {bot.cfg.total_budget:.0f}U，已用: {bot._total_filled_usdt:.2f}U")
                print("用法: budget 8000")
            else:
                try:
                    new_budget = float(parts[1])
                    if new_budget <= 0:
                        print("预算必须 > 0")
                    else:
                        bot.set_budget(new_budget)
                        print(f"总预算已设为 {new_budget:.0f}U")
                except ValueError:
                    print("无效数字，用法: budget 8000")

        elif cmd == "status":
            paused = "暂停" if bot.is_paused else "运行中"
            remaining = bot._remaining_budget()
            active = len(bot._active_orders)
            print(
                f"状态: {paused}\n"
                f"预算: {bot._total_filled_usdt:.2f} / {bot.cfg.total_budget:.0f}U "
                f"(剩余 {remaining:.2f}U)\n"
                f"活跃挂单: {active}\n"
                f"裸露仓位: {bot.naked_exposure:.4f}"
            )
            for oid, order in bot._active_orders.items():
                print(f"  买{order.level_idx}: price={order.price}, qty={order.qty:.2f}, "
                      f"hedged={order.hedged_qty:.2f}, id={oid}")

        elif cmd == "help":
            _print_help()

        else:
            print(f"未知命令: {cmd}，输入 help 查看帮助")


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
            logger.info("命令行指定总预算: %.0fU", budget_override)
        except ValueError:
            print(f"用法: python run.py [总预算USDT]  例如: python run.py 8000")
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
    logger.info("挂单范围: 买%d ~ 买%d | budget=%sU, 单笔<=%s%%金额, <=%s%%档深",
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
        level_desc = ", ".join(f"买{k}={int(v*100)}%" for k, v in bot._LEVEL_WEIGHTS.items())
        notifier.notify_start(cfg.symbol_spot, cfg.total_budget, level_desc, testnet)

    # ── 信号处理 ──
    def shutdown(signum, _frame):
        sig_name = signal.Signals(signum).name
        logger.info("收到信号 %s，准备退出...", sig_name)
        bot.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ── 控制服务器（Unix socket，始终启动） ──
    ctrl = ControlServer(bot)
    ctrl.start()

    # ── 命令行交互线程（仅 TTY 模式） ──
    if sys.stdin.isatty():
        # TTY 模式下默认暂停，等用户输入 start 后开始
        bot.pause()
        print("机器人已就绪，输入 start 开始挂单")
        cmd_thread = threading.Thread(target=_cmd_loop, args=(bot,), daemon=True)
        cmd_thread.start()
    else:
        logger.info("非交互模式（无 TTY），直接开始运行")

    # ── 运行 ──
    try:
        bot.run()
    finally:
        ctrl.stop()
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

        # 飞书停止通知
        if notifier:
            notifier.notify_stop(bot._total_filled_usdt, cfg.total_budget, bot.naked_exposure)

        ws.stop()
        trade_log.close()
        logger.info("套利机器人已完全退出")


if __name__ == "__main__":
    main()
