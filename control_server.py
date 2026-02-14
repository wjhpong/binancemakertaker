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

    def __init__(
        self,
        bot: SpotFuturesArbitrageBot,
        account_name: str = "",
        account_label: str = "",
        exchange: str = "binance",
        mode: str = "single",
        spot_exchange: str = "",
        futures_exchange: str = "",
        spot_account_label: str = "",
        futures_account_label: str = "",
    ) -> None:
        self.bot = bot
        self._account_name = account_name
        self._account_label = account_label
        self._exchange = exchange
        self._mode = mode
        self._spot_exchange = spot_exchange
        self._futures_exchange = futures_exchange
        self._spot_account_label = spot_account_label
        self._futures_account_label = futures_account_label
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
        conn.settimeout(25)  # 防止单次请求挂起
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
            with bot._close_task_lock:
                close_running = bot._close_task_running
            if close_running:
                return {"ok": False, "msg": "平仓任务进行中，禁止恢复开仓"}
            reset_done = False
            reset_hint = ""
            if args:
                try:
                    new_budget = float(args[0])
                    if new_budget <= 0:
                        return {"ok": False, "msg": "预算必须 > 0"}
                    bot.set_budget(new_budget)
                    # 仅在安全条件下才允许重置统计（暂停且无挂单且无平仓任务）
                    reset_done = bot.reset_round_counters_if_safe()
                    if not reset_done:
                        reset_hint = "（运行中仅更新预算，不重置统计）"
                except ValueError:
                    return {"ok": False, "msg": "无效数字"}
            if bot.is_paused:
                bot.resume()
                if args:
                    if reset_done:
                        return {"ok": True, "msg": f"新一轮开仓: 预算 {new_budget:.6f} 币，已恢复挂单"}
                    return {"ok": True, "msg": f"预算已更新为 {new_budget:.6f} 币，已恢复挂单{reset_hint}"}
                return {"ok": True, "msg": "已恢复挂单"}
            if args:
                return {"ok": True, "msg": f"预算已更新为 {new_budget:.6f} 币，已在运行中{reset_hint}"}
            return {"ok": True, "msg": "已在运行中"}

        elif cmd == "pause":
            if not bot.is_paused:
                bot.pause()
                return {"ok": True, "msg": "已暂停，全部挂单将撤销"}
            return {"ok": True, "msg": "已经是暂停状态"}

        elif cmd == "stop":
            bot.stop()
            return {"ok": True, "msg": "正在停止..."}

        elif cmd == "pause_close":
            ok, msg = bot.pause_close_task()
            return {"ok": ok, "msg": msg}

        elif cmd == "resume_close":
            ok, msg = bot.resume_close_task()
            return {"ok": ok, "msg": msg}

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
                    "min_spread_bps": snap.get("min_spread_bps"),
                }
            try:
                bps = float(args[0])
                bot.set_min_spread_bps(bps)
                return {
                    "ok": True,
                    "msg": f"最小spread已设为 {bps:.4f} bps",
                    "min_spread_bps": bps,
                }
            except ValueError:
                return {"ok": False, "msg": "无效数字"}

        elif cmd == "spread_info":
            return bot.get_spread_snapshot()

        elif cmd == "finish_open":
            summary = bot.finish_open()
            summary["ok"] = True
            summary["msg"] = summary.get("msg", "已终止开仓")
            return summary

        elif cmd == "finish_close":
            summary = bot.finish_close()
            summary["ok"] = True
            summary["msg"] = summary.get("msg", "已终止平仓")
            return summary

        elif cmd == "transfer":
            # 划转: transfer <asset> <amount> <direction>
            # direction: to_spot | to_future
            if len(args) < 3:
                return {"ok": False, "msg": "用法: transfer <asset> <amount> <to_spot|to_future>"}
            asset = str(args[0]).upper()
            try:
                amount = float(args[1])
                if amount <= 0:
                    return {"ok": False, "msg": "金额必须 > 0"}
            except ValueError:
                return {"ok": False, "msg": "无效金额"}
            direction = str(args[2]).lower()
            if direction not in ("to_spot", "to_future"):
                return {"ok": False, "msg": "方向必须为 to_spot 或 to_future"}
            if not hasattr(bot.adapter, "internal_transfer"):
                return {"ok": False, "msg": "当前交易所不支持划转"}
            try:
                result = bot.adapter.internal_transfer(asset, amount, direction)
                label = "合约→现货" if direction == "to_spot" else "现货→合约"
                return {"ok": True, "msg": f"划转成功: {amount} {asset} ({label})", "result": result}
            except Exception as e:
                return {"ok": False, "msg": f"划转失败: {e}"}

        elif cmd == "status":
            snap = bot.get_status_snapshot()
            snap["ok"] = True
            snap["account_name"] = self._account_name
            snap["account_label"] = self._account_label
            snap["exchange"] = self._exchange
            snap["mode"] = self._mode
            if self._mode == "cross":
                snap["spot_exchange"] = self._spot_exchange
                snap["futures_exchange"] = self._futures_exchange
                snap["spot_account_label"] = self._spot_account_label
                snap["futures_account_label"] = self._futures_account_label
            return snap

        else:
            return {"ok": False, "msg": f"未知命令: {cmd}"}
