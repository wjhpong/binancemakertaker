#!/usr/bin/env python3
"""启动入口 —— 加载配置、初始化组件、信号处理、优雅退出。
远程控制通过 ctl.py + Unix socket 实现。

用法:
    python run.py                    # 使用 config.yaml 中的 active_account
    python run.py main               # 指定账户名
    python run.py main 5000          # 指定账户名 + 覆盖预算
"""

from __future__ import annotations

import logging
import signal
import sys
from dataclasses import replace

from arbitrage_bot import SpotFuturesArbitrageBot
from config import ConfigError, load_config
from control_server import ControlServer
from feishu_notifier import FeishuNotifier
from trade_logger import TradeLogger

logger = logging.getLogger("run")


def main() -> None:
    # ── 预初始化日志（确保 config 加载阶段也有日志输出）──
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.StreamHandler()],
    )

    # ── 命令行参数：[账户名] [总预算覆盖] ──
    account_name_arg = None
    budget_override = None
    args = sys.argv[1:]
    for arg in args:
        try:
            budget_override = float(arg)
        except ValueError:
            account_name_arg = arg

    if account_name_arg:
        logger.info("命令行指定账户: %s", account_name_arg)
    if budget_override is not None:
        logger.info("命令行指定总预算: %.6f 币", budget_override)

    # ── 加载配置 ──
    try:
        account, fee, cfg, log_config = load_config(account_name=account_name_arg)
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

    exchange = log_config.get("exchange", "binance")
    mode = log_config.get("mode", "single")

    logger.info("=" * 60)
    if mode == "cross":
        logger.info("跨所套利机器人启动")
    else:
        logger.info("同所做市套利机器人启动")
    logger.info("模式: %s | 交易所: %s", mode, exchange)
    logger.info("账户: %s (%s)", account.name, account.label)
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
        notifier.account_label = account.label
        logger.info("飞书通知已启用")

    # ── 初始化组件 ──
    trade_log = TradeLogger(account=account.name)
    ws_managers = []  # 跟踪所有 WS manager，用于退出时清理

    if mode == "cross":
        # ── 跨所模式：分别初始化现货端和合约端 ──
        from cross_exchange_adapter import CrossExchangeAdapter
        cross_cfg = log_config["cross_config"]

        # — 现货端 —
        if cross_cfg.spot_exchange == "gate":
            from gate_ws_manager import GateWSManager
            from gate_adapter import GateAdapter
            spot_ws = GateWSManager(
                symbol=cfg.symbol_spot,
                api_key=cross_cfg.spot_account.api_key,
                api_secret=cross_cfg.spot_account.api_secret,
            )
            spot_ws.start()
            spot_adapter = GateAdapter(
                api_key=cross_cfg.spot_account.api_key,
                api_secret=cross_cfg.spot_account.api_secret,
                price_cache=spot_ws.price_cache,
            )
            ws_managers.append(spot_ws)
        elif cross_cfg.spot_exchange == "aster":
            from aster_ws_manager import AsterWSManager
            from aster_adapter import AsterAdapter
            spot_ws = AsterWSManager(
                symbol=cfg.symbol_spot,
                api_key=cross_cfg.spot_account.api_key,
            )
            spot_ws.start()
            spot_adapter = AsterAdapter(
                api_key=cross_cfg.spot_account.api_key,
                api_secret=cross_cfg.spot_account.api_secret,
                price_cache=spot_ws.price_cache,
            )
            ws_managers.append(spot_ws)
        elif cross_cfg.spot_exchange == "bitget":
            from bitget_ws_manager import BitgetWSManager
            from bitget_adapter import BitgetAdapter
            spot_ws = BitgetWSManager(
                symbol=cfg.symbol_spot,
                api_key=cross_cfg.spot_account.api_key,
                api_secret=cross_cfg.spot_account.api_secret,
                passphrase=cross_cfg.spot_account.passphrase,
            )
            spot_ws.start()
            spot_adapter = BitgetAdapter(
                api_key=cross_cfg.spot_account.api_key,
                api_secret=cross_cfg.spot_account.api_secret,
                passphrase=cross_cfg.spot_account.passphrase,
                price_cache=spot_ws.price_cache,
            )
            ws_managers.append(spot_ws)
        else:  # binance
            from ws_manager import WSManager
            from binance_adapter import BinanceAdapter
            spot_ws = WSManager(
                symbol=cfg.symbol_spot,
                api_key=cross_cfg.spot_account.api_key,
            )
            spot_ws.start()
            spot_adapter = BinanceAdapter(
                api_key=cross_cfg.spot_account.api_key,
                api_secret=cross_cfg.spot_account.api_secret,
                price_cache=spot_ws.price_cache,
            )
            ws_managers.append(spot_ws)

        # — 合约端 —
        if cross_cfg.futures_exchange == "aster":
            from aster_ws_manager import AsterWSManager as FutAsterWS
            from aster_adapter import AsterAdapter as FutAsterAdapter
            fut_ws = FutAsterWS(
                symbol=cfg.symbol_fut,
                api_key=cross_cfg.futures_account.api_key,
            )
            fut_ws.start()
            futures_adapter = FutAsterAdapter(
                api_key=cross_cfg.futures_account.api_key,
                api_secret=cross_cfg.futures_account.api_secret,
                price_cache=fut_ws.price_cache,
            )
            ws_managers.append(fut_ws)
        else:  # binance
            from ws_manager import WSManager as FutWSManager
            from binance_adapter import BinanceAdapter as FutBinanceAdapter
            fut_ws = FutWSManager(
                symbol=cfg.symbol_fut,
                api_key=cross_cfg.futures_account.api_key,
            )
            fut_ws.start()
            futures_adapter = FutBinanceAdapter(
                api_key=cross_cfg.futures_account.api_key,
                api_secret=cross_cfg.futures_account.api_secret,
                price_cache=fut_ws.price_cache,
            )
            ws_managers.append(fut_ws)

        # — 组合复合 adapter —
        adapter = CrossExchangeAdapter(spot_adapter, futures_adapter)
        fill_queue = spot_ws.fill_queue  # 成交事件来自现货端

        logger.info("跨所模式: 现货=%s(%s) | 合约=%s(%s)",
                     cross_cfg.spot_exchange, cross_cfg.spot_account.label,
                     cross_cfg.futures_exchange, cross_cfg.futures_account.label)

    elif exchange == "aster":
        # ── 单所: Aster ──
        from aster_ws_manager import AsterWSManager
        from aster_adapter import AsterAdapter
        ws = AsterWSManager(
            symbol=cfg.symbol_spot,
            api_key=account.api_key,
        )
        ws.start()
        adapter = AsterAdapter(
            api_key=account.api_key,
            api_secret=account.api_secret,
            price_cache=ws.price_cache,
        )
        fill_queue = ws.fill_queue
        ws_managers.append(ws)
    else:
        # ── 单所: Binance ──
        from ws_manager import WSManager
        from binance_adapter import BinanceAdapter
        ws = WSManager(
            symbol=cfg.symbol_spot,
            api_key=account.api_key,
        )
        ws.start()
        adapter = BinanceAdapter(
            api_key=account.api_key,
            api_secret=account.api_secret,
            price_cache=ws.price_cache,
        )
        fill_queue = ws.fill_queue
        ws_managers.append(ws)

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
        fill_queue=fill_queue,
    )
    if mode == "cross":
        # 跨所模式优先降低 WS 漏包窗口，缩短 REST 对账间隔
        bot.fh.set_rest_reconcile_interval(2.0)
        logger.info("跨所模式: REST 对账间隔已调整为 %.1fs", 2.0)
    bot.notifier = notifier
    if notifier:
        if mode == "cross":
            cross_cfg = log_config["cross_config"]
            notifier.mode = "cross"
            notifier.spot_exchange = cross_cfg.spot_exchange
            notifier.futures_exchange = cross_cfg.futures_exchange
        notifier.notify_start(cfg.symbol_spot)

    # ── 信号处理 ──
    def shutdown(signum, _frame):
        sig_name = signal.Signals(signum).name
        logger.info("收到信号 %s，准备退出...", sig_name)
        bot.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ── 控制服务器（Unix socket，供 ctl.py 远程控制） ──
    ctrl_kwargs = dict(account_name=account.name, account_label=account.label, exchange=exchange, mode=mode)
    if mode == "cross":
        cross_cfg = log_config["cross_config"]
        ctrl_kwargs["spot_exchange"] = cross_cfg.spot_exchange
        ctrl_kwargs["futures_exchange"] = cross_cfg.futures_exchange
        ctrl_kwargs["spot_account_label"] = cross_cfg.spot_account.label
        ctrl_kwargs["futures_account_label"] = cross_cfg.futures_account.label
    ctrl = ControlServer(bot, **ctrl_kwargs)
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

        for wm in ws_managers:
            wm.stop()
        trade_log.close()
        logger.info("套利机器人已完全退出")


if __name__ == "__main__":
    main()
