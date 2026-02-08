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
    python ctl.py spread auto

交互模式:
    python ctl.py
"""

from __future__ import annotations

import json
import subprocess
import sys

SSH_HOST = "tixian"  # ~/.ssh/config 中定义的 EC2 host
SOCK_PATH = "/tmp/arb-bot.sock"


def send_cmd(cmd: str, args: list[str] | None = None) -> dict:
    """通过 SSH 发送命令到 EC2 上的 Unix socket。"""
    payload = json.dumps({"cmd": cmd, "args": args or []})
    # 用 socat 连接 Unix socket
    remote_cmd = f'echo {repr(payload)} | socat - UNIX-CONNECT:{SOCK_PATH}'
    for attempt in (1, 2):
        result = subprocess.run(
            ["ssh", SSH_HOST, remote_cmd],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            break
        err = result.stderr.strip()
        no_service = "No such file" in err or "Connection refused" in err
        if cmd == "start" and attempt == 1 and no_service:
            # 兜底：服务已停时，先拉起 systemd 服务再重试一次 socket
            subprocess.run(
                ["ssh", SSH_HOST, "sudo systemctl start arb-bot"],
                capture_output=True, text=True, timeout=15,
            )
            continue
        if no_service:
            return {"ok": False, "msg": "机器人未运行或控制服务未启动"}
        return {"ok": False, "msg": f"SSH 错误: {err}"}

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

        print(f"状态: {state}")
        print(f"当前方向: {direction}")
        print(f"预算: {resp['used']:.6f} / {resp['budget']:.6f} 币 (剩余 {resp['remaining']:.6f} 币)")

        # 根据方向显示进度
        if close_running:
            # 平仓模式：显示卖出进度
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
        mode = resp.get("spread_mode", "auto")
        print(f"Spread模式: {mode}")
        print(f"最小利润门槛: {resp.get('min_profit_bps', 0.0):.4f} bps")
        print(f"当前最小spread: {resp.get('min_spread_bps', 0.0):.4f} bps")
        spot_avg = resp.get("spot_avg_price")
        perp_avg = resp.get("perp_avg_price")
        print(f"现货买入均价: {spot_avg:.6f}" if spot_avg is not None else "现货买入均价: -")
        print(f"永续卖出均价: {perp_avg:.6f}" if perp_avg is not None else "永续卖出均价: -")
        priced_base = resp.get("perp_avg_priced_base", 0.0)
        if priced_base > 0:
            print(f"永续均价覆盖量: {priced_base:.6f} 币")
        print(f"裸露仓位: {resp['naked_exposure']:.4f}")

        # 上次平仓结果（平仓已结束但有历史记录）
        if not close_running and close_task.get("target_qty"):
            print(f"上次平仓: {close_task.get('msg', '-')} "
                  f"(卖出 {close_task.get('spot_sold', 0.0):.6f} / "
                  f"{close_task.get('target_qty', 0.0):.6f} 币)")

    elif "budget" in resp and "paused" not in resp:
        print(f"预算: {resp['used']:.6f} / {resp['budget']:.6f} 币 (剩余 {resp['remaining']:.6f} 币)")
    elif "min_spread_bps" in resp:
        mode = resp.get("spread_mode", "auto")
        print(f"Spread模式: {mode}")
        print(f"最小利润门槛: {resp.get('min_profit_bps', 0.0):.4f} bps")
        print(f"当前最小spread: {resp.get('min_spread_bps', 0.0):.4f} bps")


_MENU = [
    ("查看状态", "status", []),
    ("开始挂单", None, []),         # 选方向：开仓 / 平仓
    ("暂停挂单", "pause", []),
    ("查看预算", "budget", []),
    ("修改预算", None, []),          # 需要额外输入
    ("停止机器人", "stop", []),
    ("修改Spread", None, []),       # 需要额外输入 bps
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
        if choice == 8:
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
                    qty = input("请输入本次开仓总币数量（直接回车=不修改）: ").strip()
                except (EOFError, KeyboardInterrupt):
                    print()
                    break
                if qty:
                    budget_resp = send_cmd("budget", [qty])
                    print_resp(budget_resp)
                    if not budget_resp.get("ok"):
                        _print_menu()
                        continue
                resp = send_cmd("start")
                print_resp(resp)
            elif d == "b":
                try:
                    qty = input("请输入平仓数量（币）: ").strip()
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

        # 修改预算 —— 需要输入币数量
        if choice == 5:
            try:
                amt = input("请输入新预算（币数量）: ").strip()
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
        if choice == 7:
            try:
                bps = input("请输入最小spread(bps)，或输入 auto 切回自动模式: ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not bps:
                continue
            resp = send_cmd("spread", [bps])
            print_resp(resp)
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
