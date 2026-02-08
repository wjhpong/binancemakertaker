"""配置加载：从 .env 读 API Key，从 config.yaml 读策略参数。"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Tuple

import yaml
from dotenv import load_dotenv

from arbitrage_bot import FeeConfig, StrategyConfig

_PROJECT_DIR = Path(__file__).resolve().parent


def load_config(
    env_path: Path | None = None,
    yaml_path: Path | None = None,
) -> Tuple[str, str, bool, FeeConfig, StrategyConfig, dict]:
    """
    返回 (api_key, api_secret, testnet, fee_config, strategy_config, log_config)
    """
    # ── .env ──
    env_file = env_path or _PROJECT_DIR / ".env"
    if env_file.exists():
        load_dotenv(env_file)

    api_key = os.environ.get("BINANCE_API_KEY", "")
    api_secret = os.environ.get("BINANCE_API_SECRET", "")
    testnet = os.environ.get("BINANCE_TESTNET", "true").lower() in ("1", "true", "yes")

    if not api_key or not api_secret:
        print("错误：请在 .env 文件中配置 BINANCE_API_KEY 和 BINANCE_API_SECRET", file=sys.stderr)
        sys.exit(1)

    # ── config.yaml ──
    yaml_file = yaml_path or _PROJECT_DIR / "config.yaml"
    if not yaml_file.exists():
        print(f"错误：找不到配置文件 {yaml_file}", file=sys.stderr)
        sys.exit(1)

    try:
        with open(yaml_file, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except yaml.YAMLError as e:
        print(f"错误：config.yaml 解析失败: {e}", file=sys.stderr)
        sys.exit(1)

    if not isinstance(raw, dict):
        print("错误：config.yaml 格式不正确，应为字典结构", file=sys.stderr)
        sys.exit(1)

    # 费率
    fee_raw = raw.get("fee", {})
    fee_defaults = FeeConfig()
    fee = FeeConfig(
        spot_maker=float(fee_raw.get("spot_maker", fee_defaults.spot_maker)),
        fut_taker=float(fee_raw.get("fut_taker", fee_defaults.fut_taker)),
        min_spread_bps=float(fee_raw.get("min_spread_bps", fee_defaults.min_spread_bps)),
    )

    # 策略
    strat_raw = raw.get("strategy", {})
    cfg = StrategyConfig(
        symbol_spot=strat_raw.get("symbol_spot", "BTCUSDT"),
        symbol_fut=strat_raw.get("symbol_fut", "BTCUSDT"),
        tick_size_spot=float(strat_raw.get("tick_size_spot", 0.01)),
        total_budget=float(strat_raw.get("total_budget", 10000)),
        budget_pct=float(strat_raw.get("budget_pct", 0.01)),
        depth_ratio=float(strat_raw.get("depth_ratio", 0.3)),
        min_order_qty=float(strat_raw.get("min_order_qty", 0.00001)),
        lot_size=float(strat_raw.get("lot_size", 0.00001)),
        min_level=int(strat_raw.get("min_level", 2)),
        max_level=int(strat_raw.get("max_level", 5)),
        poll_interval_sec=float(strat_raw.get("poll_interval_sec", 0.2)),
        reprice_bps=float(strat_raw.get("reprice_bps", 0.5)),
        max_retry=int(strat_raw.get("max_retry", 3)),
    )

    # 日志
    log_raw = raw.get("logging", {})
    log_config = {
        "level": log_raw.get("level", "INFO"),
        "file": log_raw.get("file", "arbitrage.log"),
    }

    return api_key, api_secret, testnet, fee, cfg, log_config
