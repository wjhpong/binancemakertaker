#!/usr/bin/env python3
"""本地命令行客户端 —— 通过 SSH 远程控制 EC2 上的套利机器人。

用法:
    python ctl.py status
    python ctl.py start
    python ctl.py pause
    python ctl.py stop
    python ctl.py budget
    python ctl.py budget 8000

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
    result = subprocess.run(
        ["ssh", SSH_HOST, remote_cmd],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        err = result.stderr.strip()
        if "No such file" in err or "Connection refused" in err:
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
        print(f"状态: {state}")
        print(f"预算: {resp['used']:.6f} / {resp['budget']:.6f} 币 (剩余 {resp['remaining']:.6f} 币)")
        print(f"裸露仓位: {resp['naked_exposure']}")
        orders = resp.get("active_orders", [])
        if orders:
            print(f"活跃挂单 ({len(orders)}):")
            for o in orders:
                print(f"  买{o['level']}: price={o['price']}, qty={o['qty']:.2f}, "
                      f"hedged={o['hedged']:.2f}, id={o['id']}")
        else:
            print("活跃挂单: 无")

    elif "budget" in resp and "paused" not in resp:
        print(f"预算: {resp['used']:.6f} / {resp['budget']:.6f} 币 (剩余 {resp['remaining']:.6f} 币)")


_MENU = [
    ("查看状态", "status", []),
    ("开始挂单", "start", []),
    ("暂停挂单", "pause", []),
    ("查看预算", "budget", []),
    ("修改预算", None, []),      # 需要额外输入
    ("停止机器人", "stop", []),
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
            line = input("请选择 [1-7]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not line:
            continue

        try:
            choice = int(line)
        except ValueError:
            print("请输入数字 1-7")
            continue

        if choice < 1 or choice > len(_MENU):
            print("请输入数字 1-7")
            continue

        label, cmd, args = _MENU[choice - 1]

        # 退出
        if choice == 7:
            break

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
