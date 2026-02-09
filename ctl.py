#!/usr/bin/env python3
"""本地命令行客户端 —— 通过 SSH 远程控制 EC2 上的套利机器人。

用法:
    python ctl.py status
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
import subprocess
import sys
import time

SSH_HOST = "tixian"  # ~/.ssh/config 中定义的 EC2 host
SOCK_PATH = "/tmp/arb-bot.sock"


def send_cmd(cmd: str, args: list[str] | None = None) -> dict:
    """通过 SSH 发送命令到 EC2 上的 Unix socket。"""
    payload = json.dumps({"cmd": cmd, "args": args or []})
    # 用 socat 连接 Unix socket
    remote_cmd = f'echo {repr(payload)} | socat - UNIX-CONNECT:{SOCK_PATH}'
    for attempt in (1, 2):
        try:
            result = subprocess.run(
                ["ssh", SSH_HOST, remote_cmd],
                capture_output=True, text=True, timeout=10,
            )
        except subprocess.TimeoutExpired:
            return {"ok": False, "msg": "SSH 连接超时，请检查网络或 EC2 状态"}
        if result.returncode == 0:
            break
        err = result.stderr.strip()
        no_service = "No such file" in err or "Connection refused" in err
        if cmd == "start" and attempt == 1 and no_service:
            # 兜底：服务已停时，先拉起 systemd 服务再重试一次 socket
            print("机器人未运行，正在启动服务...")
            try:
                subprocess.run(
                    ["ssh", SSH_HOST, "sudo systemctl start arb-bot"],
                    capture_output=True, text=True, timeout=15,
                )
            except subprocess.TimeoutExpired:
                return {"ok": False, "msg": "启动服务超时，请检查 EC2 状态"}
            time.sleep(3)  # 等待 bot 初始化完成、socket 就绪
            continue
        if no_service:
            return {"ok": False, "msg": "机器人未运行或控制服务未启动"}
        return {"ok": False, "msg": f"SSH 错误: {err}"}
    else:
        # 两次都失败
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

        # 判断当前方向
        if close_running:
            direction = "平仓（卖出）"
        else:
            direction = "开仓（买入）"

        symbol = resp.get("symbol", "未知")
        print(f"代币: {symbol}")
        print(f"状态: {state}")
        print(f"当前方向: {direction}")
        print(f"预算: {resp['used']:.6f} / {resp['budget']:.6f} 币 (剩余 {resp['remaining']:.6f} 币)")

        # 根据方向显示进度
        if close_running:
            # 平仓模式：显示卖出进度
            close_paused = close_task.get("paused", False)
            if close_paused:
                print("⏸ 平仓已暂停")
            target = close_task.get("target_qty", 0.0)
            sold = close_task.get("spot_sold", 0.0)
            perp_bought = close_task.get("perp_bought", 0.0)
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
        else:
            # 开仓模式：显示买入进度
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
        actual_fut = resp.get("actual_futures_position")
        print("── 交易所实际持仓 ──")
        if actual_spot is not None:
            print(f"  现货余额: {actual_spot:.6f} 币")
        else:
            print("  现货余额: 查询失败")
        if actual_fut is not None:
            direction = "空头" if actual_fut < 0 else ("多头" if actual_fut > 0 else "无仓位")
            print(f"  永续合约: {abs(actual_fut):.6f} 币 ({direction})")
        else:
            print("  永续合约: 查询失败")

        # 上次平仓结果（平仓已结束但有历史记录）
        if not close_running and close_task.get("target_qty"):
            print(f"上次平仓: {close_task.get('msg', '-')} "
                  f"(卖出 {close_task.get('spot_sold', 0.0):.6f} / "
                  f"{close_task.get('target_qty', 0.0):.6f} 币)")

    elif "budget" in resp and "paused" not in resp:
        print(f"预算: {resp['used']:.6f} / {resp['budget']:.6f} 币 (剩余 {resp['remaining']:.6f} 币)")
    elif "min_spread_bps" in resp:
        print(f"最小spread: {resp.get('min_spread_bps', 0.0):.4f} bps")


_MENU = [
    ("查看状态", "status", []),
    ("开始挂单", None, []),         # 选方向：开仓 / 平仓
    ("暂停", None, []),             # 智能暂停：开仓→暂停开仓，平仓→暂停平仓
    ("恢复", None, []),             # 智能恢复：开仓→恢复开仓，平仓→恢复平仓
    ("终止", None, []),             # 终止开仓/平仓，推送汇总
    ("查看数量", "budget", []),
    ("修改数量", None, []),          # 需要额外输入
    ("停止机器人", "stop", []),
    ("修改Spread", None, []),       # 需要额外输入 bps
    ("修改代币", None, []),          # 切换交易对
    ("退出", None, []),
]


def _print_menu() -> None:
    print()
    for i, (label, *_) in enumerate(_MENU, 1):
        print(f"  {i}. {label}")
    print()


def interactive() -> None:
    """交互式数字菜单。"""
    print("套利机器人远程控制")
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

        # 开始挂单 —— 选方向
        if choice == 2:
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
                print(f"  当前持仓: 现货已买入 {spot_filled:.6f} 币, 永续已对冲 {perp_hedged:.6f} 币")
                if spot_filled <= 0:
                    print("  ⚠ 当前无持仓，无法平仓")
                    _print_menu()
                    continue
                try:
                    qty = input(f"请输入平仓数量（币，最大 {spot_filled:.6f}）: ").strip()
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
        if choice == 3:
            status = send_cmd("status")
            close_task = (status.get("close_task") or {})
            if close_task.get("running"):
                resp = send_cmd("pause_close")
            else:
                resp = send_cmd("pause")
            print_resp(resp)
            _print_menu()
            continue

        # 恢复 —— 先查状态判断方向
        if choice == 4:
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
        if choice == 5:
            status = send_cmd("status")
            close_task = (status.get("close_task") or {})
            if close_task.get("running"):
                # 当前在平仓中
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
            else:
                # 当前在开仓中
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
        if choice == 7:
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
        if choice == 9:
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
        if choice == 10:
            # 读取当前 symbol
            try:
                result = subprocess.run(
                    ["ssh", SSH_HOST, "grep 'symbol_spot' /home/ubuntu/arbitrage-bot/config.yaml"],
                    capture_output=True, text=True, timeout=10,
                )
                current = result.stdout.strip().split(":")[-1].strip() if result.returncode == 0 else "未知"
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
            remote_cmd = (
                f"cd /home/ubuntu/arbitrage-bot && "
                f"sed -i 's/symbol_spot: .*/symbol_spot: {new_symbol}/' config.yaml && "
                f"sed -i 's/symbol_fut: .*/symbol_fut: {new_symbol}/' config.yaml && "
                f"echo OK"
            )
            try:
                result = subprocess.run(
                    ["ssh", SSH_HOST, remote_cmd],
                    capture_output=True, text=True, timeout=10,
                )
                if "OK" in result.stdout:
                    print(f"✅ 配置已切换到 {new_symbol}")
                    # 自动重启服务使新代币生效
                    print("正在重启机器人服务...")
                    try:
                        subprocess.run(
                            ["ssh", SSH_HOST, "sudo systemctl restart arb-bot"],
                            capture_output=True, text=True, timeout=15,
                        )
                        time.sleep(3)
                        print("✅ 机器人已重启，新代币已生效")
                    except subprocess.TimeoutExpired:
                        print("⚠ 重启超时，请手动执行: sudo systemctl restart arb-bot")
                else:
                    print(f"⚠ 修改失败")
                    print(result.stderr)
            except subprocess.TimeoutExpired:
                print("⚠ 操作超时，请手动检查 EC2 状态")
            except Exception as e:
                print(f"⚠ 操作失败: {e}")

            _print_menu()
            continue

        resp = send_cmd(cmd, args)
        print_resp(resp)
        _print_menu()


def main() -> None:
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        args = sys.argv[2:]
        resp = send_cmd(cmd, args)
        print_resp(resp)
    else:
        interactive()


if __name__ == "__main__":
    main()
