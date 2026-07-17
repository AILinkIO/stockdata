"""只读查询（web 页面 / API 共用）+ watchlist 写入。全部纯 PG，绝不触碰 baostock。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd

from .pool import get_pool

KLINE_FREQS = ("5", "30", "d", "w")


def add_watch(code: str, note: str = "") -> None:
    with get_pool().connection() as conn:
        conn.execute(
            "INSERT INTO watchlist (code, note) VALUES (%s, %s) "
            "ON CONFLICT (code) DO NOTHING",
            (code, note),
        )


def remove_watch(code: str) -> None:
    with get_pool().connection() as conn:
        conn.execute("DELETE FROM watchlist WHERE code = %s", (code,))


def watchlist_overview() -> list[dict[str, Any]]:
    """关注列表 + 各码名称与四种 K 线（日/周/5分/30分）水位新鲜度。"""
    sql = """
        SELECT w.code,
               COALESCE(s.code_name, '') AS code_name,
               w.added_at,
               d.last_date   AS k_d_until,
               wk.last_date  AS k_w_until,
               m5.last_date  AS k_5_until,
               m30.last_date AS k_30_until
        FROM watchlist w
        LEFT JOIN security s ON s.code = w.code
        LEFT JOIN sync_watermark d   ON d.code   = w.code AND d.dataset   = 'k_d'
        LEFT JOIN sync_watermark wk  ON wk.code  = w.code AND wk.dataset  = 'k_w'
        LEFT JOIN sync_watermark m5  ON m5.code  = w.code AND m5.dataset  = 'k_5'
        LEFT JOIN sync_watermark m30 ON m30.code = w.code AND m30.dataset = 'k_30'
        ORDER BY w.code
    """
    with get_pool().connection() as conn:
        rows = conn.execute(sql).fetchall()
    return [
        {
            "code": r[0], "code_name": r[1], "added_at": r[2],
            "k_d_until": r[3], "k_w_until": r[4],
            "k_5_until": r[5], "k_30_until": r[6],
        }
        for r in rows
    ]


def security_exists(code: str) -> bool:
    with get_pool().connection() as conn:
        return conn.execute(
            "SELECT 1 FROM security WHERE code = %s", (code,)
        ).fetchone() is not None


def security_name(code: str) -> str:
    with get_pool().connection() as conn:
        row = conn.execute(
            "SELECT code_name FROM security WHERE code = %s", (code,)
        ).fetchone()
    return row[0] if row else ""


def load_kline(code: str, frequency: str, start: date, end: date) -> pd.DataFrame:
    """K 线（不复权原始值）。日/周返回 trade_date 索引，分钟返回 bar_time（上海时区）。"""
    if frequency in ("d", "w"):
        sql = """
            SELECT trade_date AS t, open, high, low, close, volume, amount
            FROM kline
            WHERE code = %s AND frequency = %s AND trade_date BETWEEN %s AND %s
            ORDER BY trade_date
        """
    else:
        sql = """
            SELECT (bar_time AT TIME ZONE 'Asia/Shanghai') AS t,
                   open, high, low, close, volume, amount
            FROM kline_minute
            WHERE code = %s AND frequency = %s
              AND bar_time >= %s::date AND bar_time < (%s::date + 1)
            ORDER BY bar_time
        """
    with get_pool().connection() as conn:
        rows = conn.execute(sql, (code, frequency, start, end)).fetchall()
    return pd.DataFrame(
        rows, columns=["t", "open", "high", "low", "close", "volume", "amount"]
    )


def security_info(code: str) -> dict[str, Any] | None:
    """单票基本信息：security + 最新行业快照。"""
    sql = """
        SELECT s.code, s.code_name, s.ipo_date, s.out_date, s.type, s.status,
               i.industry, i.industry_classification, i.snap_date
        FROM security s
        LEFT JOIN LATERAL (
            SELECT industry, industry_classification, snap_date
            FROM stock_industry WHERE code = s.code
            ORDER BY snap_date DESC LIMIT 1
        ) i ON true
        WHERE s.code = %s
    """
    with get_pool().connection() as conn:
        r = conn.execute(sql, (code,)).fetchone()
    if r is None:
        return None
    return {
        "code": r[0], "code_name": r[1],
        "ipo_date": _iso(r[2]), "out_date": _iso(r[3]),
        "type": r[4], "status": r[5],
        "industry": r[6], "industry_classification": r[7],
        "industry_snap_date": _iso(r[8]),
    }


def financial_reports(code: str, report_type: str) -> list[dict[str, Any]]:
    """某类财报（profit/operation/growth/balance/cash_flow/dupont/
    performance_express/forecast），按报告期倒序。"""
    with get_pool().connection() as conn:
        rows = conn.execute(
            "SELECT stat_date, pub_date, metrics FROM financial_report "
            "WHERE code = %s AND report_type = %s ORDER BY stat_date DESC",
            (code, report_type),
        ).fetchall()
    return [
        {"stat_date": _iso(r[0]), "pub_date": _iso(r[1]), "metrics": r[2]}
        for r in rows
    ]


def dividends(code: str) -> list[dict[str, Any]]:
    """分红除权记录，按预案公告日倒序。"""
    with get_pool().connection() as conn:
        rows = conn.execute(
            "SELECT plan_announce_date, year_type, operate_date, detail "
            "FROM dividend WHERE code = %s ORDER BY plan_announce_date DESC",
            (code,),
        ).fetchall()
    return [
        {
            "plan_announce_date": _iso(r[0]), "year_type": r[1],
            "operate_date": _iso(r[2]), "detail": r[3],
        }
        for r in rows
    ]


def load_adjust_factors(code: str) -> pd.DataFrame:
    with get_pool().connection() as conn:
        rows = conn.execute(
            "SELECT divid_operate_date, back_adjust_factor FROM adjust_factor "
            "WHERE code = %s ORDER BY divid_operate_date",
            (code,),
        ).fetchall()
    return pd.DataFrame(rows, columns=["divid_operate_date", "back_adjust_factor"])


def recent_runs(limit: int = 10) -> list[dict[str, Any]]:
    with get_pool().connection() as conn:
        rows = conn.execute(
            "SELECT id, started_at, finished_at, status, params, stats "
            "FROM sync_run ORDER BY id DESC LIMIT %s",
            (limit,),
        ).fetchall()
    return [
        {
            "id": r[0],
            "started_at": _iso(r[1]), "finished_at": _iso(r[2]),
            "status": r[3], "params": r[4], "stats": r[5],
        }
        for r in rows
    ]


def market_watermarks() -> dict[str, dict[str, Any]]:
    """市场级数据集（code=''）的水位，按 dataset 索引。"""
    with get_pool().connection() as conn:
        rows = conn.execute(
            "SELECT dataset, last_date, last_synced_at "
            "FROM sync_watermark WHERE code = ''"
        ).fetchall()
    return {
        r[0]: {"last_date": _iso(r[1]), "last_synced_at": _iso(r[2])}
        for r in rows
    }


def watermark_summary() -> dict[str, Any]:
    """全库水位概览：每数据集的覆盖码数与最旧/最新 last_date。"""
    with get_pool().connection() as conn:
        rows = conn.execute(
            "SELECT dataset, count(*), min(last_date), max(last_date), max(last_synced_at) "
            "FROM sync_watermark GROUP BY dataset ORDER BY dataset"
        ).fetchall()
        total = conn.execute(
            "SELECT count(*) FROM security WHERE type = 1 AND status = 1"
        ).fetchone()[0]
    return {
        "total_active_codes": total,
        "datasets": [
            {
                "dataset": r[0], "codes": r[1],
                "min_last": _iso(r[2]), "max_last": _iso(r[3]),
                "last_synced_at": _iso(r[4]),
            }
            for r in rows
        ],
    }


def _iso(v: datetime | date | None) -> str | None:
    return v.isoformat() if v is not None else None
