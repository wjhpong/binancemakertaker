"""飞书群机器人 Webhook 通知器 —— 仅推送成交进度摘要。"""

from __future__ import annotations

import json
import logging
import threading
import urllib.request

logger = logging.getLogger(__name__)


class FeishuNotifier:
    """通过飞书 Incoming Webhook 发送消息（纯标准库，无额外依赖）。"""

    def __init__(self, webhook_url: str) -> None:
        self._url = webhook_url
        self._lock = threading.Lock()

    # ── 底层发送 ──────────────────────────────────────────────

    def _post(self, payload: dict) -> bool:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self._url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                body = json.loads(resp.read())
                if body.get("code") != 0:
                    logger.warning("飞书返回错误: %s", body)
                    return False
                return True
        except Exception:
            logger.exception("飞书 Webhook 发送失败")
            return False

    def send_text(self, text: str) -> bool:
        payload = {"msg_type": "text", "content": {"text": text}}
        return self._post(payload)

    def notify_start(self, symbol: str, budget_base: float) -> None:
        text = (
            f"[机器人启动] {symbol}\n"
            f"预算: {budget_base:.4f} 币"
        )
        threading.Thread(target=self.send_text, args=(text,), daemon=True).start()

    def notify_pause(self, symbol: str) -> None:
        text = f"[机器人暂停] {symbol}\n挂单已暂停，等待恢复指令"
        threading.Thread(target=self.send_text, args=(text,), daemon=True).start()

    def notify_resume(self, symbol: str) -> None:
        text = f"[机器人恢复] {symbol}\n已恢复挂单"
        threading.Thread(target=self.send_text, args=(text,), daemon=True).start()

    def notify_progress(
        self,
        symbol: str,
        total_spot_filled_base: float,
        total_perp_hedged_base: float,
    ) -> None:
        """推送累计进度（仅现货成交量和永续对冲量）。"""
        text = (
            f"[套利进度] {symbol}\n"
            f"现货累计成交: {total_spot_filled_base:.4f} 币\n"
            f"永续累计对冲: {total_perp_hedged_base:.4f} 币"
        )
        threading.Thread(target=self.send_text, args=(text,), daemon=True).start()

    def notify_finish(self, summary: dict) -> None:
        """终止时推送汇总信息（开仓终止或平仓终止）。"""
        action = summary.get("action", "终止")
        symbol = summary.get("symbol", "")
        spot_avg = summary.get("spot_avg_price")
        perp_avg = summary.get("perp_avg_price")
        spot_avg_str = f"{spot_avg:.6f}" if spot_avg else "-"
        perp_avg_str = f"{perp_avg:.6f}" if perp_avg else "-"

        if action == "终止开仓":
            spot_qty = summary.get("spot_filled_base", 0.0)
            perp_qty = summary.get("perp_hedged_base", 0.0)
            naked = summary.get("naked_exposure", 0.0)
            lines = [
                f"[终止开仓] {symbol}",
                f"现货买入: {spot_qty:.4f} 币",
                f"永续卖出: {perp_qty:.4f} 币",
                f"现货均价: {spot_avg_str}",
                f"永续均价: {perp_avg_str}",
            ]
            if naked > 1e-12:
                lines.append(f"裸露仓位: {naked:.4f} 币")
        else:
            spot_qty = summary.get("spot_sold", 0.0)
            perp_qty = summary.get("perp_bought", 0.0)
            pending = summary.get("pending_hedge", 0.0)
            lines = [
                f"[终止平仓] {symbol}",
                f"现货卖出: {spot_qty:.4f} 币",
                f"永续买入: {perp_qty:.4f} 币",
                f"现货均价: {spot_avg_str}",
                f"永续均价: {perp_avg_str}",
            ]
            if pending > 1e-12:
                lines.append(f"待对冲: {pending:.4f} 币")

        text = "\n".join(lines)
        threading.Thread(target=self.send_text, args=(text,), daemon=True).start()
