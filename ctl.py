#!/usr/bin/env python3
"""套利机器人控制客户端 —— 自动检测本地/远程模式。

在 EC2 上运行时直接连接 Unix socket（推荐）；
在本地 Mac 上运行时通过 SSH 转发。

用法:
    python ctl.py status
    python ctl.py spread_info     # 查看实时价差率
    python ctl.py start
    python ctl.py start 10000
    python ctl.py pause
    python ctl.py stop
    python ctl.py budget
    python ctl.py budget 8000
    python ctl.py spread
    python ctl.py spread 1.5

交互模式:
    python ctl.py
"""

from __future__ import annotations

import json
import os
import socket as socket_mod
import subprocess
import sys
import time

SSH_HOST = "tixian"  # ~/.ssh/config 中定义的 EC2 host
SOCK_PATH = "/tmp/arb-bot.sock"
REMOTE_CONFIG = "/home/ubuntu/arbitrage-bot/config.yaml"
LOCAL_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")

# 自动检测：socket 文件存在说明在 EC2 本地运行
IS_LOCAL = os.path.exists(SOCK_PATH)


def _get_config_path() -> str:
    """返回 config.yaml 路径（本地或远程）。"""
    return LOCAL_CONFIG if IS_LOCAL else REMOTE_CONFIG


def _read_config_text() -> str:
    """读取 config.yaml 内容。"""
    if IS_LOCAL:
        try:
            with open(LOCAL_CONFIG, "r") as f:
                return f.read()
        except Exception:
            return ""
    try:
        result = subprocess.run(
            ["ssh", SSH_HOST, f"cat {REMOTE_CONFIG}"],
            capture_output=True, text=True, timeout=10,
        )
        return result.stdout if result.returncode == 0 else ""
    except Exception:
        return ""


def _run_cmd(cmd_str: str, timeout: int = 10) -> tuple[bool, str]:
    """执行 shell 命令（本地或 SSH）。"""
    if IS_LOCAL:
        try:
            r = subprocess.run(cmd_str, shell=True, capture_output=True, text=True, timeout=timeout)
            return r.returncode == 0, r.stdout.strip()
        except Exception:
            return False, ""
    try:
        r = subprocess.run(["ssh", SSH_HOST, cmd_str], capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0, r.stdout.strip()
    except Exception:
        return False, ""


def _restart_service() -> bool:
    """重启 arb-bot 服务。"""
    ok, _ = _run_cmd("sudo systemctl restart arb-bot", timeout=15)
    return ok


def _read_remote_accounts() -> list[dict]:
    """读取 config.yaml 的账户列表。

    Returns:
        [{"name": "main", "label": "主账户", "budget": 4000, "active": True}, ...]
    """
    try:
        text = _read_config_text()
        if not text:
            return []
        import yaml
        raw = yaml.safe_load(text)
        if not isinstance(raw, dict):
            return []
        accounts_raw = raw.get("accounts")
        if not accounts_raw or not isinstance(accounts_raw, dict):
            return []
        active = raw.get("active_account", "")
        accounts = []
        for name, acct in accounts_raw.items():
            accounts.append({
                "name": name,
                "label": acct.get("label", name),
                "budget": acct.get("total_budget", 0),
                "active": name == active,
            })
        return accounts
    except Exception:
        return []


def _read_remote_exchange() -> str:
    """读取当前交易所配置。跨所模式返回 'gate+binance' 格式。"""
    try:
        text = _read_config_text()
        mode = "single"
        exchange = "binance"
        spot_ex = ""
        fut_ex = ""
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("mode:"):
                mode = stripped.split(":")[-1].strip()
            elif stripped.startswith("exchange:") and not stripped.startswith("spot_exchange") and not stripped.startswith("futures_exchange"):
                if not line.startswith(" "):  # 顶层 exchange
                    exchange = stripped.split(":")[-1].strip()
            elif stripped.startswith("spot_exchange:"):
                spot_ex = stripped.split(":")[-1].strip()
            elif stripped.startswith("futures_exchange:"):
                fut_ex = stripped.split(":")[-1].strip()
        if mode == "cross" and spot_ex and fut_ex:
            return f"{spot_ex}+{fut_ex}"
        return exchange
    except Exception:
        pass
    return "binance"


def _read_remote_mode() -> str:
    """读取 config.yaml 的 mode 字段。"""
    try:
        text = _read_config_text()
        for line in text.splitlines():
            if line.startswith("mode:"):
                return line.split(":")[-1].strip()
    except Exception:
        pass
    return "single"


def _set_remote_exchange(exchange: str) -> bool:
    """修改 config.yaml 的 exchange 字段（单所模式）。"""
    if exchange not in ("binance", "aster"):
        print(f"  ⚠ 不支持的交易所: {exchange}")
        return False
    config_path = _get_config_path()
    ok, out = _run_cmd(f"sed -i 's/^exchange: .*/exchange: {exchange}/' {config_path} && echo OK")
    return "OK" in out


def _set_remote_exchange_mode(selection: str) -> bool:
    """根据选择设置单所或跨所模式。

    selection 格式:
      - "binance" / "aster" → 单所模式
      - "gate+binance" / "bitget+binance" → 跨所模式
    """
    config_path = _get_config_path()
    if "+" in selection:
        # 跨所模式
        parts = selection.split("+")
        spot_ex = parts[0]
        fut_ex = parts[1]
        # 现货交易所 → 默认账户映射
        spot_account_map = {
            "gate": "gate1",
            "bitget": "bitget1",
        }
        spot_acct = spot_account_map.get(spot_ex, spot_ex + "1")
        cmds = [
            f"sed -i 's/^mode: .*/mode: cross/' {config_path}",
            f"sed -i 's/^  spot_exchange: .*/  spot_exchange: {spot_ex}/' {config_path}",
            f"sed -i 's/^  futures_exchange: .*/  futures_exchange: {fut_ex}/' {config_path}",
            f"sed -i 's/^  spot_account: .*/  spot_account: {spot_acct}/' {config_path}",
        ]
        ok, out = _run_cmd(" && ".join(cmds) + " && echo OK")
        return "OK" in out
    else:
        # 单所模式
        cmds = [
            f"sed -i 's/^mode: .*/mode: single/' {config_path}",
            f"sed -i 's/^exchange: .*/exchange: {selection}/' {config_path}",
        ]
        ok, out = _run_cmd(" && ".join(cmds) + " && echo OK")
        return "OK" in out


def _set_remote_active_account(account_name: str) -> bool:
    """修改 config.yaml 的 active_account 字段。"""
    if not account_name.replace("_", "").replace("-", "").isalnum():
        print(f"  ⚠ 账户名包含非法字符: {account_name}")
        return False
    config_path = _get_config_path()
    ok, out = _run_cmd(f"sed -i 's/^active_account: .*/active_account: {account_name}/' {config_path} && echo OK")
    return "OK" in out


def _select_account() -> str | None:
    """让用户选择账户，返回账户名；返回 None 表示取消。"""
    accounts = _read_remote_accounts()
    if not accounts:
        print("  ⚠ 无法读取账户列表，将使用默认账户")
        return ""  # 空字符串表示使用 config.yaml 中的默认值

    if len(accounts) == 1:
        acct = accounts[0]
        print(f"  仅一个账户: {acct['label']} (预算 {acct['budget']} 币)")
        return acct["name"]

    print("  请选择账户:")
    for i, acct in enumerate(accounts, 1):
        marker = " ← 当前" if acct["active"] else ""
        print(f"    {i}. {acct['label']} (预算 {acct['budget']} 币){marker}")

    try:
        choice_str = input(f"  请选择 [1-{len(accounts)}]: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        return None

    try:
        idx = int(choice_str) - 1
        if 0 <= idx < len(accounts):
            return accounts[idx]["name"]
    except ValueError:
        pass

    print("  无效选择")
    return None


def _send_local(payload: str) -> str:
    """直接通过 Unix socket 发送命令（EC2 本地模式）。"""
    sock = socket_mod.socket(socket_mod.AF_UNIX, socket_mod.SOCK_STREAM)
    sock.settimeout(25)
    sock.connect(SOCK_PATH)
    sock.sendall(payload.encode("utf-8") + b"\n")
    data = b""
    while True:
        chunk = sock.recv(8192)
        if not chunk:
            break
        data += chunk
        if b"\n" in data or len(data) > 65536:
            break
    sock.close()
    return data.decode("utf-8").strip()


def send_cmd(cmd: str, args: list[str] | None = None) -> dict:
    """发送命令到机器人控制服务。自动选择本地直连或 SSH 转发。"""
    payload = json.dumps({"cmd": cmd, "args": args or []})

    if IS_LOCAL:
        # EC2 本地模式：直连 Unix socket
        for attempt in (1, 2):
            try:
                out = _send_local(payload)
                if out:
                    return json.loads(out)
                return {"ok": False, "msg": "无响应"}
            except FileNotFoundError:
                if cmd == "start" and attempt == 1:
                    print("机器人未运行，正在启动服务...")
                    subprocess.run(["sudo", "systemctl", "start", "arb-bot"],
                                   capture_output=True, timeout=15)
                    time.sleep(3)
                    continue
                return {"ok": False, "msg": "机器人未运行或控制服务未启动"}
            except (ConnectionRefusedError, OSError) as e:
                return {"ok": False, "msg": f"连接失败: {e}"}
            except json.JSONDecodeError as e:
                return {"ok": False, "msg": f"解析失败: {e}"}
        return {"ok": False, "msg": "服务启动后仍无法连接，请检查日志"}

    # 远程模式：通过 SSH + socat
    remote_cmd = f'echo {repr(payload)} | socat - UNIX-CONNECT:{SOCK_PATH}'
    for attempt in (1, 2):
        try:
            result = subprocess.run(
                ["ssh", SSH_HOST, remote_cmd],
                capture_output=True, text=True, timeout=30,
            )
        except subprocess.TimeoutExpired:
            return {"ok": False, "msg": "SSH 连接超时，请检查网络或 EC2 状态"}
        if result.returncode == 0:
            break
        err = result.stderr.strip()
        no_service = "No such file" in err or "Connection refused" in err
        if cmd == "start" and attempt == 1 and no_service:
            print("机器人未运行，正在启动服务...")
            try:
                subprocess.run(
                    ["ssh", SSH_HOST, "sudo systemctl start arb-bot"],
                    capture_output=True, text=True, timeout=15,
                )
            except subprocess.TimeoutExpired:
                return {"ok": False, "msg": "启动服务超时，请检查 EC2 状态"}
            time.sleep(3)
            continue
        if no_service:
            return {"ok": False, "msg": "机器人未运行或控制服务未启动"}
        return {"ok": False, "msg": f"SSH 错误: {err}"}
    else:
        return {"ok": False, "msg": "服务启动后仍无法连接，请检查 EC2 上的日志"}

    out = result.stdout.strip()
    if not out:
        return {"ok": False, "msg": "无响应"}
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return {"ok": False, "msg": f"解析失败: {out}"}


def print_resp(resp: dict) -> None:
    """格式化输出响应。"""
    if "msg" in resp:
        print(resp["msg"])

    if "paused" in resp:
        state = "暂停" if resp["paused"] else "运行中"
        close_task = resp.get("close_task") or {}
        close_running = close_task.get("running", False)
        close_has_record = close_task.get("target_qty", 0.0) > 1e-12

        # 判断当前方向
        if close_running or close_has_record:
            direction = "平仓（卖出）"
            if not close_running:
                state = "平仓已结束"
        elif resp["paused"] and resp.get("spot_filled_base", 0.0) < 1e-12:
            direction = "待命"
        else:
            direction = "开仓（买入）"

        symbol = resp.get("symbol", "未知")
        exchange_name = resp.get("exchange", "")
        acct_label = resp.get("account_label", "")
        acct_name = resp.get("account_name", "")
        run_mode = resp.get("mode", "single")
        if exchange_name:
            if run_mode == "cross":
                spot_ex = resp.get("spot_exchange", "")
                fut_ex = resp.get("futures_exchange", "")
                spot_label = resp.get("spot_account_label", "")
                fut_label = resp.get("futures_account_label", "")
                print(f"模式: 跨所套利")
                print(f"  现货: {spot_ex} ({spot_label})")
                print(f"  合约: {fut_ex} ({fut_label})")
            else:
                print(f"交易所: {exchange_name}")
        if acct_label and run_mode != "cross":
            print(f"账户: {acct_label} ({acct_name})")
        print(f"代币: {symbol}")
        print(f"状态: {state}")
        print(f"当前方向: {direction}")

        # 根据方向显示进度
        if close_running or close_has_record:
            # 平仓模式
            close_paused = close_task.get("paused", False)
            if close_paused:
                print("⏸ 平仓已暂停")
            target = close_task.get("target_qty", 0.0)
            sold = close_task.get("spot_sold", 0.0)
            perp_bought = close_task.get("perp_bought", 0.0)
            print(f"平仓目标: {target:.6f} 币")
            print(f"卖出进度: 已卖出 {sold:.6f} / {target:.6f} 币")
            print(f"永续已买入对冲: {perp_bought:.6f} 币")
            pending = close_task.get("pending_hedge", 0.0)
            if pending > 1e-12:
                print(f"待对冲: {pending:.6f} 币")
            print(f"平仓状态: {close_task.get('msg', '-')}")
            # 显示平仓活跃卖单
            close_orders = close_task.get("open_orders") or []
            if close_orders:
                print(f"活跃卖单 ({len(close_orders)}):")
                for o in close_orders:
                    print(
                        f"  卖单: price={o.get('price')}, qty={o.get('qty', 0.0):.2f}, "
                        f"filled={o.get('filled', 0.0):.2f}, id={o.get('id')}"
                    )
            else:
                print("活跃卖单: 无")
        elif direction == "待命":
            # 待命模式：不显示开仓细节
            pass
        else:
            # 开仓模式
            print(f"开仓预算: {resp['used']:.6f} / {resp['budget']:.6f} 币 (剩余 {resp['remaining']:.6f} 币)")
            print(f"买入进度: 已买入 {resp.get('spot_filled_base', 0.0):.6f} 币")
            print(f"永续已卖出对冲: {resp.get('perp_hedged_base', 0.0):.6f} 币")
            # 显示开仓活跃买单
            orders = resp.get("active_orders", [])
            if orders:
                print(f"活跃买单 ({len(orders)}):")
                for o in orders:
                    cur = o.get("current_level")
                    if cur is not None:
                        level_text = f"买{o['level']}(当前买{cur})"
                    else:
                        level_text = f"买{o['level']}(当前买5外)"
                    print(f"  {level_text}: price={o['price']}, qty={o['qty']:.2f}, "
                          f"hedged={o['hedged']:.2f}, id={o['id']}")
            else:
                print("活跃买单: 无")

        # 通用信息
        print(f"最小spread: {resp.get('min_spread_bps', 0.0):.4f} bps")
        if close_running or close_has_record:
            # 平仓模式：显示卖出均价和买入均价
            spot_avg = close_task.get("spot_sell_avg_price")
            perp_avg = close_task.get("perp_buy_avg_price")
            print(f"现货卖出均价: {spot_avg:.6f}" if spot_avg is not None else "现货卖出均价: -")
            print(f"永续买入均价: {perp_avg:.6f}" if perp_avg is not None else "永续买入均价: -")
            print(f"裸露仓位: {resp['naked_exposure']:.4f}")
        elif direction != "待命":
            # 开仓模式：显示买入均价和卖出均价
            spot_avg = resp.get("spot_avg_price")
            perp_avg = resp.get("perp_avg_price")
            print(f"现货买入均价: {spot_avg:.6f}" if spot_avg is not None else "现货买入均价: -")
            print(f"永续卖出均价: {perp_avg:.6f}" if perp_avg is not None else "永续卖出均价: -")
            priced_base = resp.get("perp_avg_priced_base", 0.0)
            if priced_base > 0:
                print(f"永续均价覆盖量: {priced_base:.6f} 币")
            print(f"裸露仓位: {resp['naked_exposure']:.4f}")

        # 交易所实际持仓
        actual_spot = resp.get("actual_spot_balance")
        actual_earn = resp.get("actual_earn_balance")
        actual_fut = resp.get("actual_futures_position")
        print("── 交易所实际持仓 ──")
        if actual_spot is not None:
            print(f"  现货余额: {actual_spot:.6f} 币")
        else:
            print("  现货余额: 查询失败")
        if actual_earn is not None and actual_earn > 0:
            print(f"  理财余额: {actual_earn:.6f} 币")
        elif actual_earn is not None:
            pass  # 理财为0时不显示
        else:
            print("  理财余额: 查询失败")
        if actual_spot is not None and actual_earn is not None:
            total_spot = actual_spot + actual_earn
            if actual_earn > 0:
                print(f"  现货合计: {total_spot:.6f} 币")
        if actual_fut is not None:
            fut_direction = "空头" if actual_fut < 0 else ("多头" if actual_fut > 0 else "无仓位")
            print(f"  永续合约: {abs(actual_fut):.6f} 币 ({fut_direction})")
        else:
            print("  永续合约: 查询失败")

        # 合约账户详情（Aster 等支持时显示）
        fa = resp.get("futures_account", {})
        if fa:
            print("── 合约账户详情 ──")
            wallet = fa.get("wallet_balance", 0)
            unrealized = fa.get("unrealized_pnl", 0)
            margin_bal = fa.get("margin_balance", 0)
            avail = fa.get("available_balance", 0)
            lev = fa.get("leverage", 0)
            entry = fa.get("entry_price", 0)
            mark = fa.get("mark_price", 0)
            liq = fa.get("liquidation_price", 0)
            liq_dist = fa.get("liq_distance_pct", 0)
            notional = fa.get("notional", 0)
            max_qty = fa.get("max_open_qty", 0)
            max_notional = fa.get("max_open_notional", 0)
            print(f"  钱包余额:     {wallet:,.2f} USDT")
            print(f"  未实现盈亏:   {unrealized:+,.2f} USDT")
            print(f"  保证金余额:   {margin_bal:,.2f} USDT")
            print(f"  可用余额:     {avail:,.2f} USDT")
            if lev > 0:
                print(f"  杠杆:         {lev}x")
            if entry > 0:
                print(f"  开仓均价:     {entry:.6f}")
            if mark > 0:
                print(f"  标记价格:     {mark:.6f}")
            if notional > 0:
                print(f"  持仓名义:     {notional:,.2f} USDT")
            if liq > 0:
                print(f"  强平价格:     {liq:.6f}  (距离 {liq_dist:.1f}%)")
            if max_qty > 0:
                print(f"  还可开仓:     {max_qty:,.2f} 币 ({max_notional:,.2f} USDT)")
            elif avail <= 0:
                print("  还可开仓:     0 (可用余额不足)")


    elif "open_levels" in resp:
        # spread_info 响应 —— 根据当前方向只显示对应的 spread
        symbol = resp.get("symbol", "?")
        fut_bid = resp.get("fut_bid", 0)
        fut_ask = resp.get("fut_ask", 0)
        min_bps = resp.get("min_spread_bps", 0)
        direction = resp.get("direction", "open")

        print(f"代币: {symbol}  |  门槛: {min_bps:.2f}bp  |  方向: {'平仓' if direction == 'close' else '开仓'}")

        if direction == "close":
            # ── 平仓方向: -A+B (卖现货ask1 + 买永续ask1)
            close_levels = resp.get("close_levels", [])
            if close_levels:
                lv = close_levels[0]
                s = lv["spread_bps"]
                mark = "✓" if s >= min_bps else "✗"
                print(f"平仓 -A+B: 卖一 {lv['price']:.6f} / 永续Ask {fut_ask:.6f}  spread: {s:+.2f}bp  {mark}")
            else:
                print("平仓 -A+B: 无行情")
        else:
            # ── 开仓方向: +A-B (买现货bid1 + 卖永续bid1)
            open_levels = resp.get("open_levels", [])
            if open_levels:
                lv = open_levels[0]
                s = lv["spread_bps"]
                mark = "✓" if s >= min_bps else "✗"
                print(f"开仓 +A-B: 买一 {lv['price']:.6f} / 永续Bid {fut_bid:.6f}  spread: {s:+.2f}bp  {mark}")
            else:
                print("开仓 +A-B: 无行情")

    elif "budget" in resp and "paused" not in resp:
        print(f"预算: {resp['used']:.6f} / {resp['budget']:.6f} 币 (剩余 {resp['remaining']:.6f} 币)")
    elif "min_spread_bps" in resp:
        print(f"最小spread: {resp.get('min_spread_bps', 0.0):.4f} bps")


_MENU = [
    ("查看状态", "status", []),
    ("查看价差", "spread_info", []),  # 实时价差率
    ("开始挂单", None, []),         # 选方向：开仓 / 平仓
    ("暂停", None, []),             # 智能暂停：开仓→暂停开仓，平仓→暂停平仓
    ("恢复", None, []),             # 智能恢复：开仓→恢复开仓，平仓→恢复平仓
    ("终止", None, []),             # 终止开仓/平仓，推送汇总
    ("查看数量", "budget", []),
    ("修改数量", None, []),          # 需要额外输入
    ("停止机器人", "stop", []),
    ("修改Spread", None, []),       # 需要额外输入 bps
    ("修改代币", None, []),          # 切换交易对
    ("切换账户", None, []),          # 切换账户并重启
    ("切换交易所", None, []),        # 切换 binance / aster
    ("划转", None, []),              # 合约⇄现货 内部划转
    ("退出", None, []),
]


def _print_menu() -> None:
    print()
    for i, (label, *_) in enumerate(_MENU, 1):
        print(f"  {i}. {label}")
    print()


def interactive() -> None:
    """交互式数字菜单。"""
    mode = "本地直连" if IS_LOCAL else f"SSH → {SSH_HOST}"
    print(f"套利机器人控制台 ({mode})")
    _print_menu()

    while True:
        try:
            line = input(f"请选择 [1-{len(_MENU)}]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not line:
            continue

        try:
            choice = int(line)
        except ValueError:
            print(f"请输入数字 1-{len(_MENU)}")
            continue

        if choice < 1 or choice > len(_MENU):
            print(f"请输入数字 1-{len(_MENU)}")
            continue

        label, cmd, args = _MENU[choice - 1]

        # 退出
        if choice == len(_MENU):
            break

        # 查看价差 —— 持续刷新
        if choice == 2:
            _watch_spread()
            _print_menu()
            continue

        # 开始挂单 —— 选方向
        if choice == 3:
            try:
                print("  a. 开仓（买入）")
                print("  b. 平仓（卖出）")
                d = input("请选择方向 [a/b]: ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                print()
                break

            if d == "a":
                try:
                    qty = input("请输入本次开仓预算（币数量，直接回车=不修改）: ").strip()
                except (EOFError, KeyboardInterrupt):
                    print()
                    break
                start_args = [qty] if qty else []
                resp = send_cmd("start", start_args)
                print_resp(resp)
            elif d == "b":
                # 先查询当前持仓，显示可平仓数量
                status = send_cmd("status")
                spot_filled = status.get("spot_filled_base", 0.0)
                perp_hedged = status.get("perp_hedged_base", 0.0)
                # 如果机器人内部记录为0，使用交易所实际持仓
                actual_spot = status.get("actual_spot_balance") or 0.0
                actual_earn = status.get("actual_earn_balance") or 0.0
                actual_total = actual_spot + actual_earn
                actual_fut = abs(status.get("actual_futures_position") or 0.0)
                if spot_filled > 0:
                    max_close = spot_filled
                    print(f"  当前持仓: 现货已买入 {spot_filled:.6f} 币, 永续已对冲 {perp_hedged:.6f} 币")
                elif actual_total > 0 or actual_fut > 0:
                    # 机器人内部无记录，但交易所有实际仓位
                    max_close = min(actual_total, actual_fut) if actual_total > 0 and actual_fut > 0 else max(actual_total, actual_fut)
                    print(f"  机器人内部无记录，但交易所有实际仓位:")
                    print(f"  现货(含理财): {actual_total:.6f} 币")
                    print(f"  永续空头: {actual_fut:.6f} 币")
                    print(f"  建议平仓量: {max_close:.6f} 币")
                else:
                    print("  ⚠ 当前无持仓，无法平仓")
                    _print_menu()
                    continue
                try:
                    qty = input(f"请输入平仓数量（币，最大 {max_close:.6f}）: ").strip()
                except (EOFError, KeyboardInterrupt):
                    print()
                    break
                if not qty:
                    _print_menu()
                    continue
                resp = send_cmd("close", [qty])
                if not resp.get("ok"):
                    print(f"⚠ 平仓失败: {resp.get('msg', '未知错误')}")
                else:
                    print_resp(resp)
            else:
                print("无效选择")
            _print_menu()
            continue

        # 暂停 —— 先查状态判断方向
        if choice == 4:
            status = send_cmd("status")
            close_task = (status.get("close_task") or {})
            is_paused = status.get("paused", False)
            spot_filled = status.get("spot_filled_base", 0.0)
            if is_paused and not close_task.get("running") and spot_filled < 1e-12:
                print("⚠ 当前已处于待命状态")
                _print_menu()
                continue
            if close_task.get("running"):
                resp = send_cmd("pause_close")
            else:
                resp = send_cmd("pause")
            print_resp(resp)
            _print_menu()
            continue

        # 恢复 —— 先查状态判断方向
        if choice == 5:
            status = send_cmd("status")
            close_task = (status.get("close_task") or {})
            if close_task.get("running"):
                resp = send_cmd("resume_close")
            else:
                resp = send_cmd("start")
            print_resp(resp)
            _print_menu()
            continue

        # 终止 —— 先查状态判断方向
        if choice == 6:
            status = send_cmd("status")
            close_task = (status.get("close_task") or {})
            is_paused = status.get("paused", False)
            spot_filled = status.get("spot_filled_base", 0.0)
            # 判断是否处于平仓（正在执行 或 刚结束但有记录）
            close_running = close_task.get("running", False)
            close_has_record = close_task.get("target_qty", 0.0) > 1e-12
            if close_running or close_has_record:
                # 平仓中或刚结束平仓
                sold = close_task.get("spot_sold", 0.0)
                target = close_task.get("target_qty", 0.0)
                state_str = "进行中" if close_running else "已结束"
                print(f"  平仓状态: {state_str}，已卖出 {sold:.6f} / {target:.6f} 币")
                try:
                    confirm = input("确认终止平仓？(y/n): ").strip().lower()
                except (EOFError, KeyboardInterrupt):
                    print()
                    break
                if confirm != "y":
                    print("已取消")
                    _print_menu()
                    continue
                resp = send_cmd("finish_close")
            elif is_paused and spot_filled < 1e-12:
                # 待命状态，没有开仓也没有平仓
                print("⚠ 当前处于待命状态，无需终止")
                _print_menu()
                continue
            else:
                # 当前在开仓中
                print(f"  开仓进度: 已买入 {spot_filled:.6f} 币")
                try:
                    confirm = input("确认终止开仓？(y/n): ").strip().lower()
                except (EOFError, KeyboardInterrupt):
                    print()
                    break
                if confirm != "y":
                    print("已取消")
                    _print_menu()
                    continue
                resp = send_cmd("finish_open")
            if resp.get("ok"):
                print(f"✅ {resp.get('msg', '已终止')}")
                # 显示汇总
                action = resp.get("action", "")
                symbol = resp.get("symbol", "")
                if action == "终止开仓":
                    print(f"  代币: {symbol}")
                    print(f"  现货买入: {resp.get('spot_filled_base', 0.0):.6f} 币")
                    print(f"  永续卖出: {resp.get('perp_hedged_base', 0.0):.6f} 币")
                    spot_avg = resp.get("spot_avg_price")
                    perp_avg = resp.get("perp_avg_price")
                    print(f"  现货均价: {spot_avg:.6f}" if spot_avg else "  现货均价: -")
                    print(f"  永续均价: {perp_avg:.6f}" if perp_avg else "  永续均价: -")
                    naked = resp.get("naked_exposure", 0.0)
                    if naked > 1e-12:
                        print(f"  裸露仓位: {naked:.4f} 币")
                elif action == "终止平仓":
                    print(f"  代币: {symbol}")
                    print(f"  现货卖出: {resp.get('spot_sold', 0.0):.6f} 币")
                    print(f"  永续买入: {resp.get('perp_bought', 0.0):.6f} 币")
                    spot_avg = resp.get("spot_avg_price")
                    perp_avg = resp.get("perp_avg_price")
                    print(f"  现货均价: {spot_avg:.6f}" if spot_avg else "  现货均价: -")
                    print(f"  永续均价: {perp_avg:.6f}" if perp_avg else "  永续均价: -")
                    pending = resp.get("pending_hedge", 0.0)
                    if pending > 1e-12:
                        print(f"  待对冲: {pending:.4f} 币")
            else:
                print(f"⚠ {resp.get('msg', '终止失败')}")
            _print_menu()
            continue

        # 修改数量
        if choice == 8:
            try:
                amt = input("请输入新数量（币数量）: ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not amt:
                continue
            resp = send_cmd("budget", [amt])
            print_resp(resp)
            _print_menu()
            continue

        # 修改 spread(bps)
        if choice == 10:
            try:
                bps = input("请输入最小spread(bps): ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not bps:
                continue
            resp = send_cmd("spread", [bps])
            print_resp(resp)
            _print_menu()
            continue

        # 修改代币
        if choice == 11:
            # 读取当前 symbol
            try:
                ok, out = _run_cmd(f"grep 'symbol_spot' {_get_config_path()}")
                current = out.split(":")[-1].strip() if ok and out else "未知"
            except Exception:
                current = "未知"
            print(f"  当前代币: {current}")
            try:
                token = input("请输入新代币名称（如 BTC, ETH, ASTER）: ").strip().upper()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not token:
                _print_menu()
                continue
            new_symbol = token if token.endswith("USDT") else token + "USDT"
            try:
                confirm = input(f"确认切换到 {new_symbol}？(y/n): ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if confirm != "y":
                print("已取消")
                _print_menu()
                continue

            print(f"正在修改配置为 {new_symbol}...")
            config_path = _get_config_path()
            cmd = (
                f"sed -i 's/symbol_spot: .*/symbol_spot: {new_symbol}/' {config_path} && "
                f"sed -i 's/symbol_fut: .*/symbol_fut: {new_symbol}/' {config_path} && "
                f"echo OK"
            )
            ok, out = _run_cmd(cmd)
            if "OK" in out:
                print(f"✅ 配置已切换到 {new_symbol}")
                print("正在重启机器人服务...")
                if _restart_service():
                    time.sleep(3)
                    print("✅ 机器人已重启，新代币已生效")
                else:
                    print("⚠ 重启失败，请手动执行: sudo systemctl restart arb-bot")
            else:
                print("⚠ 修改失败")

            _print_menu()
            continue

        # 切换账户
        if choice == 12:
            acct_name = _select_account()
            if acct_name is None:
                _print_menu()
                continue
            if not acct_name:
                # 无法读取账户列表
                _print_menu()
                continue
            if not _set_remote_active_account(acct_name):
                print("⚠ 修改账户配置失败")
                _print_menu()
                continue
            print(f"已切换到账户: {acct_name}，正在重启机器人服务...")
            if _restart_service():
                time.sleep(3)
                print("✅ 机器人已重启，账户切换生效")
            else:
                print("⚠ 重启失败，请手动执行: sudo systemctl restart arb-bot")
            _print_menu()
            continue

        # 切换交易所 / 模式
        if choice == 13:
            current_ex = _read_remote_exchange()
            current_mode = _read_remote_mode()
            if current_mode == "cross":
                print(f"  当前: 跨所模式 ({current_ex})")
            else:
                print(f"  当前: 单所模式 ({current_ex})")
            options = [
                ("binance", "单所"),
                ("aster", "单所"),
                ("gate+binance", "跨所"),
                ("bitget+binance", "跨所"),
            ]
            for i, (name, mode_label) in enumerate(options, 1):
                mark = " ← 当前" if name == current_ex else ""
                print(f"    {i}. {name} ({mode_label}){mark}")
            try:
                sel = input(f"选择 [1-{len(options)}，直接回车取消]: ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not sel:
                _print_menu()
                continue
            try:
                idx = int(sel) - 1
                if idx < 0 or idx >= len(options):
                    raise ValueError
                new_exchange = options[idx][0]
            except ValueError:
                print("  ⚠ 无效选择")
                _print_menu()
                continue
            if new_exchange == current_ex:
                print(f"  已经是 {new_exchange}，无需切换")
                _print_menu()
                continue
            if not _set_remote_exchange_mode(new_exchange):
                print("⚠ 修改配置失败")
                _print_menu()
                continue
            print(f"已切换到: {new_exchange}，正在重启机器人服务...")
            if _restart_service():
                time.sleep(3)
                print(f"✅ 机器人已重启，交易所切换到 {new_exchange}")
            else:
                print("⚠ 重启失败，请手动执行: sudo systemctl restart arb-bot")
            _print_menu()
            continue

        # 划转 (合约⇄现货)
        if choice == 14:
            # 跨所模式不支持划转
            status = send_cmd("status", [])
            if status.get("mode") == "cross":
                print("  ⚠ 跨所模式不支持内部划转（资金在不同交易所）")
                _print_menu()
                continue
            fa = status.get("futures_account", {})
            spot_bal = status.get("actual_spot_balance", 0) or 0
            fut_avail = fa.get("available_balance", 0)
            fut_withdraw = fa.get("max_withdraw", 0)
            print(f"  现货余额:       {spot_bal:,.4f} 币")
            print(f"  合约可用余额:   {fut_avail:,.2f} USDT")
            print(f"  合约可转出:     {fut_withdraw:,.2f} USDT")
            print()
            print("  划转方向:")
            print("    1. 合约 → 现货")
            print("    2. 现货 → 合约")
            try:
                d = input("  选择 [1/2，直接回车取消]: ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not d:
                _print_menu()
                continue
            if d == "1":
                direction = "to_spot"
                label = "合约→现货"
                max_hint = f"  (合约可转出最大: {fut_withdraw:,.2f} USDT)"
            elif d == "2":
                direction = "to_future"
                label = "现货→合约"
                max_hint = ""
            else:
                print("  ⚠ 无效选择")
                _print_menu()
                continue
            try:
                asset = input("  资产 (默认 USDT): ").strip().upper() or "USDT"
                if max_hint:
                    print(max_hint)
                amount_str = input(f"  划转数量 ({asset}): ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not amount_str:
                print("  已取消")
                _print_menu()
                continue
            try:
                amount = float(amount_str)
                if amount <= 0:
                    raise ValueError
            except ValueError:
                print("  ⚠ 无效数量")
                _print_menu()
                continue
            # 检查是否超限
            if direction == "to_spot" and asset == "USDT" and amount > fut_withdraw:
                print(f"  ⚠ 超过合约可转出上限 {fut_withdraw:,.2f} USDT")
                confirm = input(f"  仍然尝试? [y/N]: ").strip().lower()
                if confirm != "y":
                    print("  已取消")
                    _print_menu()
                    continue
            confirm = input(f"  确认划转 {amount} {asset} ({label})? [y/N]: ").strip().lower()
            if confirm != "y":
                print("  已取消")
                _print_menu()
                continue
            resp = send_cmd("transfer", [asset, str(amount), direction])
            print_resp(resp)
            _print_menu()
            continue

        resp = send_cmd(cmd, args)
        print_resp(resp)
        _print_menu()


def _watch_spread(interval: float = 1.0) -> None:
    """持续刷新 spread，Ctrl+C 退出。"""
    print(f"持续刷新 spread（每 {interval:.0f}s），Ctrl+C 退出\n")
    try:
        while True:
            resp = send_cmd("spread_info")
            # 单行覆盖刷新：\r 回到行首，清行后打印
            if "open_levels" in resp:
                symbol = resp.get("symbol", "?")
                fut_bid = resp.get("fut_bid", 0)
                fut_ask = resp.get("fut_ask", 0)
                min_bps = resp.get("min_spread_bps", 0)
                direction = resp.get("direction", "open")
                if direction == "close":
                    levels = resp.get("close_levels", [])
                    if levels:
                        lv = levels[0]
                        s = lv["spread_bps"]
                        mark = "✓" if s >= min_bps else "✗"
                        line = (f"{symbol} 平仓 | 卖一 {lv['price']:.6f} / "
                                f"永续Ask {fut_ask:.6f} | spread: {s:+.2f}bp {mark} | 门槛: {min_bps:.2f}bp")
                    else:
                        line = f"{symbol} 平仓 | 无行情"
                else:
                    levels = resp.get("open_levels", [])
                    if levels:
                        lv = levels[0]
                        s = lv["spread_bps"]
                        mark = "✓" if s >= min_bps else "✗"
                        line = (f"{symbol} 开仓 | 买一 {lv['price']:.6f} / "
                                f"永续Bid {fut_bid:.6f} | spread: {s:+.2f}bp {mark} | 门槛: {min_bps:.2f}bp")
                    else:
                        line = f"{symbol} 开仓 | 无行情"
            else:
                line = resp.get("msg", "查询失败")
            print(f"\r\033[K{line}", end="", flush=True)
            time.sleep(interval)
    except KeyboardInterrupt:
        print()  # 换行


def main() -> None:
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        args = sys.argv[2:]
        if cmd == "spread_info":
            _watch_spread()
        else:
            resp = send_cmd(cmd, args)
            print_resp(resp)
    else:
        interactive()


if __name__ == "__main__":
    main()
