"""Unix socket 控制服务器 —— 允许远程发送命令给运行中的 bot。"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from arbitrage_bot import SpotFuturesArbitrageBot

logger = logging.getLogger(__name__)

_SOCK_PATH = Path("/tmp/arb-bot.sock")


class ControlServer:
    """监听 Unix socket，接收 JSON 命令，返回 JSON 响应。"""

    def __init__(self, bot: SpotFuturesArbitrageBot) -> None:
        self.bot = bot
        self._sock: socket.socket | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        # 清理旧 socket 文件
        if _SOCK_PATH.exists():
            _SOCK_PATH.unlink()

        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(str(_SOCK_PATH))
        self._sock.listen(2)
        self._sock.settimeout(1.0)  # 允许优雅退出

        # 让所有用户可连接
        os.chmod(str(_SOCK_PATH), 0o777)

        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        logger.info("控制服务器已启动: %s", _SOCK_PATH)

    def stop(self) -> None:
        if self._sock:
            self._sock.close()
        if _SOCK_PATH.exists():
            _SOCK_PATH.unlink()
        logger.info("控制服务器已停止")

    def _serve(self) -> None:
        while self.bot.is_running:
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                self._handle(conn)
            except Exception:
                logger.exception("控制连接处理异常")
            finally:
                conn.close()

    def _handle(self, conn: socket.socket) -> None:
        data = conn.recv(4096).decode("utf-8").strip()
        if not data:
            return

        try:
            req = json.loads(data)
        except json.JSONDecodeError:
            # 兼容纯文本命令
            req = {"cmd": data.split()[0], "args": data.split()[1:]}

        cmd = req.get("cmd", "").lower()
        args = req.get("args", [])
        resp = self._dispatch(cmd, args)
        conn.sendall(json.dumps(resp, ensure_ascii=False).encode("utf-8"))

    def _dispatch(self, cmd: str, args: list) -> dict:
        bot = self.bot

        if cmd == "start":
            if args:
                try:
                    new_budget = float(args[0])
                    if new_budget <= 0:
                        return {"ok": False, "msg": "预算必须 > 0"}
                    bot.set_budget(new_budget)
                except ValueError:
                    return {"ok": False, "msg": "无效数字"}
            if bot.is_paused:
                bot.resume()
                if args:
                    return {"ok": True, "msg": f"预算已设为 {new_budget:.6f} 币，已恢复挂单"}
                return {"ok": True, "msg": "已恢复挂单"}
            if args:
                return {"ok": True, "msg": f"预算已设为 {new_budget:.6f} 币，已在运行中"}
            return {"ok": True, "msg": "已在运行中"}

        elif cmd == "pause":
            if not bot.is_paused:
                bot.pause()
                return {"ok": True, "msg": "已暂停，全部挂单将撤销"}
            return {"ok": True, "msg": "已经是暂停状态"}

        elif cmd == "stop":
            bot.stop()
            return {"ok": True, "msg": "正在停止..."}

        elif cmd == "close":
            if len(args) == 1:
                symbol = bot.cfg.symbol_spot
                qty_arg = args[0]
            elif len(args) >= 2:
                symbol = str(args[0]).upper()
                if not symbol.endswith("USDT"):
                    symbol += "USDT"
                qty_arg = args[1]
            else:
                return {"ok": False, "msg": "用法: close [symbol] qty"}
            try:
                qty = float(qty_arg)
            except ValueError:
                return {"ok": False, "msg": "无效数量"}
            ok, msg = bot.start_close_task(symbol, qty)
            return {"ok": ok, "msg": msg}

        elif cmd == "budget":
            if not args:
                snap = bot.get_status_snapshot()
                return {
                    "ok": True,
                    "budget": snap["budget"],
                    "used": snap["used"],
                    "remaining": snap["remaining"],
                }
            try:
                new_budget = float(args[0])
                if new_budget <= 0:
                    return {"ok": False, "msg": "预算必须 > 0"}
                bot.set_budget(new_budget)
                return {"ok": True, "msg": f"总预算已设为 {new_budget:.6f} 币"}
            except ValueError:
                return {"ok": False, "msg": "无效数字"}

        elif cmd == "spread":
            if not args:
                snap = bot.get_status_snapshot()
                return {
                    "ok": True,
                    "min_profit_bps": snap.get("min_profit_bps"),
                    "min_spread_bps": snap.get("min_spread_bps"),
                    "spread_mode": snap.get("spread_mode"),
                }
            arg0 = str(args[0]).lower()
            if arg0 == "auto":
                bot.clear_manual_min_spread_bps()
                snap = bot.get_status_snapshot()
                return {
                    "ok": True,
                    "msg": "最小spread已切换为自动模式",
                    "min_profit_bps": snap.get("min_profit_bps"),
                    "min_spread_bps": snap.get("min_spread_bps"),
                    "spread_mode": snap.get("spread_mode"),
                }
            try:
                bps = float(args[0])
                bot.set_manual_min_spread_bps(bps)
                snap = bot.get_status_snapshot()
                return {
                    "ok": True,
                    "msg": f"最小spread已设为手动 {bps:.4f} bps",
                    "min_profit_bps": snap.get("min_profit_bps"),
                    "min_spread_bps": snap.get("min_spread_bps"),
                    "spread_mode": snap.get("spread_mode"),
                }
            except ValueError:
                return {"ok": False, "msg": "无效数字"}

        elif cmd == "status":
            snap = bot.get_status_snapshot()
            snap["ok"] = True
            return snap

        else:
            return {"ok": False, "msg": f"未知命令: {cmd}"}
