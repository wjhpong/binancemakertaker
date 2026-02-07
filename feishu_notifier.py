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
