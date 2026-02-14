"""配置加载：从 .env 读 API Key，从 config.yaml 读策略参数。

支持多账户：config.yaml 中的 accounts 段定义多个账户，
active_account 指定当前使用的账户。向后兼容无 accounts 段的旧配置。

支持跨所模式：mode=cross 时，分别加载现货和合约两个账户。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import yaml
from dotenv import load_dotenv

from arbitrage_bot import FeeConfig, StrategyConfig

_PROJECT_DIR = Path(__file__).resolve().parent


class ConfigError(Exception):
    """配置加载失败。"""


@dataclass
class AccountConfig:
    """单个账户的配置信息。"""
    name: str           # 内部键名，如 "main"
    label: str          # 显示名称，如 "主账户"
    api_key: str
    api_secret: str
    total_budget: float
    passphrase: str = ""  # Bitget 需要 passphrase


@dataclass
class CrossExchangeConfig:
    """跨所模式配置。"""
    spot_exchange: str          # "gate", "binance", "aster"
    futures_exchange: str       # "binance", "aster"
    spot_account: AccountConfig
    futures_account: AccountConfig


def _load_account(accounts_raw: dict, name: str, strat_raw: dict) -> AccountConfig:
    """从 accounts 段加载指定账户，内部辅助函数。"""
    if name not in accounts_raw:
        available = ", ".join(accounts_raw.keys())
        raise ConfigError(f"账户 '{name}' 不在 accounts 列表中。可用账户: {available}")

    acct_raw = accounts_raw[name]
    api_key_env = acct_raw.get("api_key_env", "")
    api_secret_env = acct_raw.get("api_secret_env", "")
    api_key = os.environ.get(api_key_env, "")
    api_secret = os.environ.get(api_secret_env, "")
    if not api_key or not api_secret:
        raise ConfigError(
            f"账户 '{name}' 的 API Key 环境变量未配置: "
            f"{api_key_env}={bool(api_key)}, {api_secret_env}={bool(api_secret)}"
        )
    budget = float(acct_raw.get("total_budget", strat_raw.get("total_budget", 10000)))
    # Bitget 需要 passphrase
    passphrase_env = acct_raw.get("passphrase_env", "")
    passphrase = os.environ.get(passphrase_env, "") if passphrase_env else ""
    return AccountConfig(
        name=name,
        label=acct_raw.get("label", name),
        api_key=api_key,
        api_secret=api_secret,
        total_budget=budget,
        passphrase=passphrase,
    )


def load_config(
    env_path: Path | None = None,
    yaml_path: Path | None = None,
    account_name: str | None = None,
) -> Tuple[AccountConfig, FeeConfig, StrategyConfig, dict]:
    """加载配置，返回 (account, fee_config, strategy_config, log_config)。

    Args:
        env_path: .env 文件路径，默认为项目目录下的 .env
        yaml_path: config.yaml 文件路径
        account_name: 指定要加载的账户名。
            - 如果不传，使用 config.yaml 中的 active_account
            - 如果无 accounts 段，退回原单账户逻辑
    """
    # ── .env ──
    env_file = env_path or _PROJECT_DIR / ".env"
    if env_file.exists():
        load_dotenv(env_file)

    # ── config.yaml ──
    yaml_file = yaml_path or _PROJECT_DIR / "config.yaml"
    if not yaml_file.exists():
        raise ConfigError(f"找不到配置文件 {yaml_file}")

    try:
        with open(yaml_file, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"config.yaml 解析失败: {e}") from e

    if not isinstance(raw, dict):
        raise ConfigError("config.yaml 格式不正确，应为字典结构")

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

    # 运行模式 / 交易所
    mode = raw.get("mode", "single").lower()
    if mode not in ("single", "cross"):
        raise ConfigError(f"不支持的 mode: {mode}（仅支持 single/cross）")
    exchange = raw.get("exchange", "binance").lower()
    if exchange not in ("binance", "aster"):
        raise ConfigError(f"不支持的单所 exchange: {exchange}（仅支持 binance/aster）")

    # ── 账户解析 ──
    accounts_raw = raw.get("accounts")
    cross_config = None

    if mode == "cross":
        # 跨所模式：只校验并加载 cross_exchange 指定的两个账户，
        # 不依赖 active_account，避免被无关账户阻塞启动。
        cross_raw = raw.get("cross_exchange", {})
        if not cross_raw:
            raise ConfigError("mode=cross 时必须配置 cross_exchange 段")
        if not accounts_raw or not isinstance(accounts_raw, dict):
            raise ConfigError("跨所模式需要 accounts 段")

        spot_exchange = cross_raw.get("spot_exchange", "gate").lower()
        futures_exchange = cross_raw.get("futures_exchange", "binance").lower()
        allowed_spot = {"gate", "bitget", "binance", "aster"}
        allowed_futures = {"binance", "aster"}
        if spot_exchange not in allowed_spot:
            raise ConfigError(
                f"不支持的 spot_exchange: {spot_exchange}（仅支持 {sorted(allowed_spot)}）"
            )
        if futures_exchange not in allowed_futures:
            raise ConfigError(
                f"不支持的 futures_exchange: {futures_exchange}（仅支持 {sorted(allowed_futures)}）"
            )

        spot_acct_name = cross_raw.get("spot_account", "")
        fut_acct_name = cross_raw.get("futures_account", "")
        if not spot_acct_name or not fut_acct_name:
            raise ConfigError("cross_exchange 中 spot_account 和 futures_account 必须指定")

        spot_account = _load_account(accounts_raw, spot_acct_name, strat_raw)
        futures_account = _load_account(accounts_raw, fut_acct_name, strat_raw)
        cross_config = CrossExchangeConfig(
            spot_exchange=spot_exchange,
            futures_exchange=futures_exchange,
            spot_account=spot_account,
            futures_account=futures_account,
        )
        # 跨所模式下，用现货账户作为主账户展示与日志归属
        account = spot_account
    elif accounts_raw and isinstance(accounts_raw, dict):
        # 单所 + 多账户模式
        active = account_name or raw.get("active_account", "")
        if not active:
            active = next(iter(accounts_raw))
        if active not in accounts_raw:
            available = ", ".join(accounts_raw.keys())
            raise ConfigError(
                f"账户 '{active}' 不在 accounts 列表中。可用账户: {available}"
            )
        account = _load_account(accounts_raw, active, strat_raw)
    else:
        # 向后兼容：无 accounts 段，退回原单账户逻辑
        api_key = os.environ.get("BINANCE_API_KEY", "")
        api_secret = os.environ.get("BINANCE_API_SECRET", "")
        if not api_key or not api_secret:
            raise ConfigError("请在 .env 文件中配置 BINANCE_API_KEY 和 BINANCE_API_SECRET")
        account = AccountConfig(
            name="default",
            label="默认账户",
            api_key=api_key,
            api_secret=api_secret,
            total_budget=float(strat_raw.get("total_budget", 10000)),
        )

    cfg = StrategyConfig(
        symbol_spot=strat_raw.get("symbol_spot", "BTCUSDT"),
        symbol_fut=strat_raw.get("symbol_fut", "BTCUSDT"),
        tick_size_spot=float(strat_raw.get("tick_size_spot", 0.01)),
        total_budget=account.total_budget,
        budget_pct=float(strat_raw.get("budget_pct", 0.01)),
        depth_ratio=float(strat_raw.get("depth_ratio", 0.3)),
        min_order_qty=float(strat_raw.get("min_order_qty", 0.00001)),
        lot_size=float(strat_raw.get("lot_size", 0.00001)),
        poll_interval_sec=float(strat_raw.get("poll_interval_sec", 0.2)),
        reprice_bps=float(strat_raw.get("reprice_bps", 0.5)),
        max_retry=int(strat_raw.get("max_retry", 3)),
    )

    # 日志
    log_raw = raw.get("logging", {})
    log_config = {
        "level": log_raw.get("level", "INFO"),
        "file": log_raw.get("file", "arbitrage.log"),
        "exchange": exchange,
        "mode": mode,
    }

    # ── 跨所模式额外配置 ──
    if mode == "cross":
        assert cross_config is not None
        log_config["cross_config"] = cross_config
        log_config["exchange"] = f"{cross_config.spot_exchange}+{cross_config.futures_exchange}"

    return account, fee, cfg, log_config


def load_accounts_list(yaml_path: Path | None = None) -> List[dict]:
    """返回所有配置的账户列表，供 ctl.py 远程调用。

    Returns:
        [{"name": "main", "label": "主账户", "budget": 4000, "active": True}, ...]
    """
    yaml_file = yaml_path or _PROJECT_DIR / "config.yaml"
    if not yaml_file.exists():
        return []

    with open(yaml_file, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        return []

    accounts_raw = raw.get("accounts")
    if not accounts_raw or not isinstance(accounts_raw, dict):
        return []

    active = raw.get("active_account", "")
    result = []
    for name, acct in accounts_raw.items():
        result.append({
            "name": name,
            "label": acct.get("label", name),
            "budget": acct.get("total_budget", 0),
            "active": name == active,
        })
    return result
