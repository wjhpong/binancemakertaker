"""飞书群机器人 Webhook 通知器 —— 推送成交/对冲/异常信息。"""

from __future__ import annotations

import json
import logging
import threading
import time
import urllib.request
import urllib.error
from typing import Optional

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

    # ── 业务通知 ──────────────────────────────────────────────

    def notify_fill(
        self,
        symbol: str,
        level_idx: int,
        price: float,
        qty: float,
        filled_usdt: float,
        total_filled_usdt: float,
        total_budget: float,
    ) -> None:
        """现货买单成交通知。"""
        pct = total_filled_usdt / total_budget * 100 if total_budget > 0 else 0
        text = (
            f"[现货成交] {symbol}\n"
            f"档位: 买{level_idx} | 价格: {price} | 数量: {qty:.2f}\n"
            f"本次金额: {filled_usdt:.2f}U\n"
            f"累计进度: {total_filled_usdt:.2f} / {total_budget:.0f}U ({pct:.1f}%)"
        )
        threading.Thread(target=self.send_text, args=(text,), daemon=True).start()

    def notify_hedge(
        self,
        symbol: str,
        qty: float,
        price: Optional[float],
        success: bool,
    ) -> None:
        """合约对冲通知。"""
        if success:
            text = (
                f"[合约对冲] {symbol}\n"
                f"数量: {qty:.2f} | 价格: {price or 'N/A'}\n"
                f"状态: 成功"
            )
        else:
            text = (
                f"[对冲失败] {symbol}\n"
                f"数量: {qty:.2f}\n"
                f"状态: 失败 — 已转入裸露仓位，请检查!"
            )
        threading.Thread(target=self.send_text, args=(text,), daemon=True).start()

    def notify_start(
        self,
        symbol: str,
        total_budget: float,
        levels: str,
        testnet: bool,
    ) -> None:
        """机器人启动通知。"""
        env = "模拟盘" if testnet else "实盘"
        text = (
            f"[启动] 套利机器人已启动\n"
            f"环境: {env} | 交易对: {symbol}\n"
            f"总预算: {total_budget:.0f}U | 挂单档位: {levels}"
        )
        threading.Thread(target=self.send_text, args=(text,), daemon=True).start()

    def notify_stop(
        self,
        total_filled_usdt: float,
        total_budget: float,
        naked_exposure: float,
    ) -> None:
        """机器人停止通知。"""
        pct = total_filled_usdt / total_budget * 100 if total_budget > 0 else 0
        lines = [
            f"[停止] 套利机器人已停止",
            f"累计成交: {total_filled_usdt:.2f} / {total_budget:.0f}U ({pct:.1f}%)",
        ]
        if naked_exposure > 0:
            lines.append(f"裸露仓位: {naked_exposure:.2f} — 请手动处理!")
        text = "\n".join(lines)
        # 停止通知同步发送，确保退出前送达
        self.send_text(text)

    def notify_budget_complete(self, total_budget: float) -> None:
        """预算买满通知。"""
        text = f"[完成] 已买满 {total_budget:.0f}U 总仓位，机器人停止挂单"
        threading.Thread(target=self.send_text, args=(text,), daemon=True).start()
