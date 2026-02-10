"""飞书群机器人 Webhook 通知器 —— 推送状态变更和成交记录。"""

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
        self.account_label: str = ""  # 由 run.py 设置

    @property
    def _prefix(self) -> str:
        return f"[{self.account_label}] " if self.account_label else ""

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

    def _send_async(self, text: str) -> None:
        threading.Thread(target=self.send_text, args=(text,), daemon=True).start()

    # ── 状态变更通知 ──────────────────────────────────────────

    def notify_start(self, symbol: str) -> None:
        """机器人服务启动（不含预算）。"""
        self._send_async(f"{self._prefix}[机器人已启动] {symbol}")

    def notify_open_start(self, symbol: str, budget: float) -> None:
        """开始建仓。"""
        self._send_async(
            f"{self._prefix}[开始建仓] {symbol}\n"
            f"预算: {budget:.4f} 币"
        )

    def notify_close_start(self, symbol: str, target_qty: float) -> None:
        """开始平仓。"""
        self._send_async(
            f"{self._prefix}[开始平仓] {symbol}\n"
            f"目标: {target_qty:.4f} 币"
        )

    def notify_finish(self, summary: dict) -> None:
        """终止时推送汇总信息。"""
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
                f"{self._prefix}[终止建仓] {symbol}",
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
                f"{self._prefix}[终止平仓] {symbol}",
                f"现货卖出: {spot_qty:.4f} 币",
                f"永续买入: {perp_qty:.4f} 币",
                f"现货均价: {spot_avg_str}",
                f"永续均价: {perp_avg_str}",
            ]
            if pending > 1e-12:
                lines.append(f"待对冲: {pending:.4f} 币")

        self._send_async("\n".join(lines))

    # ── 成交记录通知 ──────────────────────────────────────────

    def notify_open_trade(
        self,
        symbol: str,
        hedge_qty: float,
        hedge_price: float | None,
        total_filled: float,
        total_budget: float,
    ) -> None:
        """开仓成交记录：现货买入 → 永续卖出对冲。"""
        price_str = f"@ {hedge_price:.6f}" if hedge_price and hedge_price > 0 else ""
        self._send_async(
            f"{self._prefix}[开仓成交] {symbol}\n"
            f"对冲: {hedge_qty:.4f} 币 {price_str}\n"
            f"累计: {total_filled:.4f} / {total_budget:.4f} 币"
        )

    def notify_close_trade(
        self,
        symbol: str,
        spot_sold_this: float,
        total_sold: float,
        total_perp_bought: float,
        target_qty: float,
    ) -> None:
        """平仓成交记录：现货卖出 + 累计进度。"""
        self._send_async(
            f"{self._prefix}[平仓成交] {symbol}\n"
            f"本次卖出: {spot_sold_this:.4f} 币\n"
            f"累计现货卖出: {total_sold:.4f} / {target_qty:.4f} 币\n"
            f"累计永续平仓: {total_perp_bought:.4f} 币"
        )
