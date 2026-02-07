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
        while self.bot._running:
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
            if bot.is_paused:
                bot.resume()
                return {"ok": True, "msg": "已恢复挂单"}
            return {"ok": True, "msg": "已在运行中"}

        elif cmd == "pause":
            if not bot.is_paused:
                bot.pause()
                return {"ok": True, "msg": "已暂停，全部挂单将撤销"}
            return {"ok": True, "msg": "已经是暂停状态"}

        elif cmd == "stop":
            bot.stop()
            return {"ok": True, "msg": "正在停止..."}

        elif cmd == "budget":
            if not args:
                return {
                    "ok": True,
                    "budget": bot.cfg.total_budget,
                    "used": round(bot._total_filled_usdt, 2),
                    "remaining": round(bot._remaining_budget(), 2),
                }
            try:
                new_budget = float(args[0])
                if new_budget <= 0:
                    return {"ok": False, "msg": "预算必须 > 0"}
                bot.set_budget(new_budget)
                return {"ok": True, "msg": f"总预算已设为 {new_budget:.0f}U"}
            except ValueError:
                return {"ok": False, "msg": "无效数字"}

        elif cmd == "status":
            orders = []
            for oid, order in bot._active_orders.items():
                orders.append({
                    "level": order.level_idx,
                    "price": order.price,
                    "qty": order.qty,
                    "hedged": order.hedged_qty,
                    "id": oid,
                })
            return {
                "ok": True,
                "paused": bot.is_paused,
                "budget": bot.cfg.total_budget,
                "used": round(bot._total_filled_usdt, 2),
                "remaining": round(bot._remaining_budget(), 2),
                "naked_exposure": round(bot.naked_exposure, 4),
                "active_orders": orders,
            }

        else:
            return {"ok": False, "msg": f"未知命令: {cmd}"}
