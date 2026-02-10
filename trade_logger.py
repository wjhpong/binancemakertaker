"""交易记录持久化 —— SQLite 存储每笔下单和对冲。"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULT_DB = Path(__file__).resolve().parent / "trades.db"


class TradeLogger:
    def __init__(self, db_path: str | Path = _DEFAULT_DB, account: str = "") -> None:
        self._account = account
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        self._migrate()
        logger.info("TradeLogger 初始化完成: %s (account=%s)", db_path, account or "default")

    def _create_tables(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL    NOT NULL,
                side      TEXT    NOT NULL,
                symbol    TEXT    NOT NULL,
                order_id  TEXT    NOT NULL,
                price     REAL,
                qty       REAL    NOT NULL,
                status    TEXT    NOT NULL,
                account   TEXT    NOT NULL DEFAULT ''
            )
        """)
        self.conn.commit()

    def _migrate(self) -> None:
        """向后兼容：旧数据库可能没有 account 列，自动添加。"""
        cursor = self.conn.execute("PRAGMA table_info(trades)")
        columns = {row["name"] for row in cursor.fetchall()}
        if "account" not in columns:
            self.conn.execute("ALTER TABLE trades ADD COLUMN account TEXT NOT NULL DEFAULT ''")
            self.conn.commit()
            logger.info("已自动迁移: trades 表新增 account 列")

    # ── 写入 ──

    def log_spot_order(self, symbol: str, order_id: str, price: float, qty: float) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO trades (timestamp, side, symbol, order_id, price, qty, status, account) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (time.time(), "spot_buy", symbol, order_id, price, qty, "placed", self._account),
            )
            self.conn.commit()

    def log_spot_fill(self, symbol: str, order_id: str, fill_price: float, fill_qty: float) -> None:
        """记录现货买单成交。"""
        with self._lock:
            self.conn.execute(
                "INSERT INTO trades (timestamp, side, symbol, order_id, price, qty, status, account) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (time.time(), "spot_buy", symbol, order_id, fill_price, fill_qty, "filled", self._account),
            )
            self.conn.commit()

    def log_hedge(
        self,
        symbol: str,
        order_id: str,
        qty: float,
        *,
        success: bool,
        price: Optional[float] = None,
    ) -> None:
        status = "hedge_ok" if success else "hedge_fail"
        with self._lock:
            self.conn.execute(
                "INSERT INTO trades (timestamp, side, symbol, order_id, price, qty, status, account) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (time.time(), "futures_sell", symbol, order_id, price, qty, status, self._account),
            )
            self.conn.commit()

    # ── 查询 ──

    def get_recent_trades(self, limit: int = 20) -> list[dict]:
        """返回最近 N 条交易记录。"""
        with self._lock:
            cursor = self.conn.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_pnl_summary(self) -> dict:
        """统计简要 P&L：总买入量、总对冲量、成功/失败次数。"""
        with self._lock:
            cursor = self.conn.execute("""
                SELECT
                    SUM(CASE WHEN side='spot_buy' AND status='filled' THEN qty ELSE 0 END) as total_bought,
                    SUM(CASE WHEN side='spot_buy' AND status='filled' THEN price * qty ELSE 0 END) as total_buy_cost,
                    SUM(CASE WHEN side='futures_sell' AND status='hedge_ok' THEN qty ELSE 0 END) as total_hedged,
                    SUM(CASE WHEN side='futures_sell' AND status='hedge_ok' THEN price * qty ELSE 0 END) as total_hedge_revenue,
                    SUM(CASE WHEN status='hedge_ok' THEN 1 ELSE 0 END) as hedge_ok_count,
                    SUM(CASE WHEN status='hedge_fail' THEN 1 ELSE 0 END) as hedge_fail_count
                FROM trades
            """)
            row = cursor.fetchone()
        total_buy = row["total_buy_cost"] or 0.0
        total_sell = row["total_hedge_revenue"] or 0.0
        return {
            "total_bought_qty": row["total_bought"] or 0.0,
            "total_buy_cost": total_buy,
            "total_hedged_qty": row["total_hedged"] or 0.0,
            "total_hedge_revenue": total_sell,
            "gross_pnl": total_sell - total_buy,
            "hedge_ok_count": row["hedge_ok_count"] or 0,
            "hedge_fail_count": row["hedge_fail_count"] or 0,
        }

    def close(self) -> None:
        with self._lock:
            self.conn.close()
        logger.info("TradeLogger 已关闭")
