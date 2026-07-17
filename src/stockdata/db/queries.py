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
    """关注列表 + 名称、四种 K 线水位、最新收盘/涨跌幅、近 30 日收盘序列。"""
    sql = """
        SELECT w.code,
               COALESCE(s.code_name, '') AS code_name,
               w.added_at,
               d.last_date   AS k_d_until,
               wk.last_date  AS k_w_until,
               m5.last_date  AS k_5_until,
               m30.last_date AS k_30_until,
               lk.close      AS last_close,
               lk.pct_chg    AS last_pct_chg,
               sp.closes     AS recent_closes
        FROM watchlist w
        LEFT JOIN security s ON s.code = w.code
        LEFT JOIN sync_watermark d   ON d.code   = w.code AND d.dataset   = 'k_d'
        LEFT JOIN sync_watermark wk  ON wk.code  = w.code AND wk.dataset  = 'k_w'
        LEFT JOIN sync_watermark m5  ON m5.code  = w.code AND m5.dataset  = 'k_5'
        LEFT JOIN sync_watermark m30 ON m30.code = w.code AND m30.dataset = 'k_30'
        LEFT JOIN LATERAL (
            SELECT close, pct_chg FROM kline
            WHERE code = w.code AND frequency = 'd'
            ORDER BY trade_date DESC LIMIT 1
        ) lk ON true
        LEFT JOIN LATERAL (
            SELECT array_agg(close ORDER BY trade_date) AS closes FROM (
                SELECT trade_date, close FROM kline
                WHERE code = w.code AND frequency = 'd'
                ORDER BY trade_date DESC LIMIT 30
            ) t
        ) sp ON true
        ORDER BY w.code
    """
    with get_pool().connection() as conn:
        rows = conn.execute(sql).fetchall()
    return [
        {
            "code": r[0], "code_name": r[1], "added_at": r[2],
            "k_d_until": r[3], "k_w_until": r[4],
            "k_5_until": r[5], "k_30_until": r[6],
            "last_close": float(r[7]) if r[7] is not None else None,
            "last_pct_chg": float(r[8]) if r[8] is not None else None,
            "recent_closes": [float(c) for c in (r[9] or []) if c is not None],
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


def health_snapshot() -> dict[str, Any]:
    """全局状态横幅数据：熔断标志 + 关注列表日 K 最大滞后交易日数。"""
    with get_pool().connection() as conn:
        halt_row = conn.execute(
            "SELECT value FROM sync_state WHERE key = 'halt'"
        ).fetchone()
        settled = conn.execute(
            "SELECT max(calendar_date) FROM trade_calendar "
            "WHERE is_trading_day AND calendar_date < CURRENT_DATE"
        ).fetchone()[0]
        lag_code, lag = None, None
        if settled is not None:
            r = conn.execute(
                """
                SELECT w.code,
                       (SELECT count(*) FROM trade_calendar tc
                        WHERE tc.is_trading_day
                          AND tc.calendar_date > COALESCE(m.last_date, '1990-01-01')
                          AND tc.calendar_date <= %s) AS lag
                FROM watchlist w
                LEFT JOIN sync_watermark m ON m.code = w.code AND m.dataset = 'k_d'
                ORDER BY lag DESC LIMIT 1
                """,
                (settled,),
            ).fetchone()
            if r is not None:
                lag_code, lag = r[0], r[1]
    return {
        "halt": halt_row[0] if halt_row else None,
        "lag_code": lag_code,
        "max_lag_days": lag,
    }


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


# ── /api/v1 数据面查询（只读，供下游拉取）──

_DAILY_KLINE_COLS = (
    "trade_date", "open", "high", "low", "close", "volume", "amount", "turn",
    "pct_chg", "preclose", "trade_status", "is_st",
    "pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm",
)
_MINUTE_KLINE_COLS = ("bar_time", "open", "high", "low", "close", "volume", "amount")


def list_securities(
    type_: int | None, status: int | None, q: str, limit: int, offset: int
) -> tuple[int, list[dict[str, Any]]]:
    where: list[str] = []
    params: list = []
    if type_ is not None:
        where.append("type = %s")
        params.append(type_)
    if status is not None:
        where.append("status = %s")
        params.append(status)
    if q:
        where.append("(code ILIKE %s OR code_name ILIKE %s)")
        params += [f"%{q}%", f"%{q}%"]
    cond = ("WHERE " + " AND ".join(where)) if where else ""
    with get_pool().connection() as conn:
        total = conn.execute(
            f"SELECT count(*) FROM security {cond}", params  # noqa: S608
        ).fetchone()[0]
        rows = conn.execute(
            f"SELECT code, code_name, ipo_date, out_date, type, status "  # noqa: S608
            f"FROM security {cond} ORDER BY code LIMIT %s OFFSET %s",
            params + [limit, offset],
        ).fetchall()
    return total, [
        {
            "code": r[0], "code_name": r[1], "ipo_date": r[2],
            "out_date": r[3], "type": r[4], "status": r[5],
        }
        for r in rows
    ]


def kline_rows(
    code: str, frequency: str, start: date | None, end: date | None, limit: int
) -> list[dict[str, Any]]:
    """全字段 K 线（原始不复权值），升序，limit 截断取区间头部。"""
    if frequency in ("d", "w"):
        cols, table, tcol = _DAILY_KLINE_COLS, "kline", "trade_date"
        time_expr = tcol
    else:
        cols, table, tcol = _MINUTE_KLINE_COLS, "kline_minute", "bar_time"
        time_expr = f"({tcol} AT TIME ZONE 'Asia/Shanghai')"
    sql = (
        f"SELECT {', '.join(c if c != tcol else f'{time_expr} AS {tcol}' for c in cols)} "  # noqa: S608
        f"FROM {table} WHERE code = %s AND frequency = %s "
        f"AND (%s::date IS NULL OR {tcol} >= %s::date) "
        f"AND (%s::date IS NULL OR {tcol} < (%s::date + 1)) "
        f"ORDER BY {tcol} LIMIT %s"
    )
    with get_pool().connection() as conn:
        rows = conn.execute(
            sql, (code, frequency, start, start, end, end, limit)
        ).fetchall()
    return [dict(zip(cols, r)) for r in rows]


def adjust_factor_rows(codes: list[str]) -> dict[str, list[dict[str, Any]]]:
    with get_pool().connection() as conn:
        rows = conn.execute(
            "SELECT code, divid_operate_date, fore_adjust_factor, "
            "back_adjust_factor, adjust_factor FROM adjust_factor "
            "WHERE code = ANY(%s) ORDER BY code, divid_operate_date",
            (codes,),
        ).fetchall()
    out: dict[str, list[dict[str, Any]]] = {c: [] for c in codes}
    for r in rows:
        out[r[0]].append({
            "divid_operate_date": r[1], "fore_adjust_factor": r[2],
            "back_adjust_factor": r[3], "adjust_factor": r[4],
        })
    return out


def trade_calendar_rows(
    start: date | None, end: date | None, only_trading: bool
) -> list[dict[str, Any]]:
    sql = (
        "SELECT calendar_date, is_trading_day FROM trade_calendar "
        "WHERE (%s::date IS NULL OR calendar_date >= %s) "
        "AND (%s::date IS NULL OR calendar_date <= %s) "
    )
    if only_trading:
        sql += "AND is_trading_day "
    sql += "ORDER BY calendar_date"
    with get_pool().connection() as conn:
        rows = conn.execute(sql, (start, start, end, end)).fetchall()
    return [{"calendar_date": r[0], "is_trading_day": r[1]} for r in rows]


def industry_rows(snap_date: date | None) -> tuple[date | None, list[dict[str, Any]]]:
    """行业分类快照；snap_date 缺省取最新一期。"""
    with get_pool().connection() as conn:
        if snap_date is None:
            row = conn.execute("SELECT max(snap_date) FROM stock_industry").fetchone()
            snap_date = row[0]
        if snap_date is None:
            return None, []
        rows = conn.execute(
            "SELECT code, industry, industry_classification FROM stock_industry "
            "WHERE snap_date = %s ORDER BY code",
            (snap_date,),
        ).fetchall()
    return snap_date, [
        {"code": r[0], "industry": r[1], "industry_classification": r[2]}
        for r in rows
    ]


def index_constituent_rows(
    index_code: str, snap_date: date | None
) -> tuple[date | None, list[dict[str, Any]]]:
    with get_pool().connection() as conn:
        if snap_date is None:
            row = conn.execute(
                "SELECT max(snap_date) FROM index_constituent WHERE index_code = %s",
                (index_code,),
            ).fetchone()
            snap_date = row[0]
        if snap_date is None:
            return None, []
        rows = conn.execute(
            "SELECT code, code_name FROM index_constituent "
            "WHERE index_code = %s AND snap_date = %s ORDER BY code",
            (index_code, snap_date),
        ).fetchall()
    return snap_date, [{"code": r[0], "code_name": r[1]} for r in rows]


def macro_rows(kind: str, start: str | None, end: str | None) -> list[dict[str, Any]]:
    """宏观序列；date_key 为文本（pubDate / 'YYYY-MM' / 'YYYY'），按文本序过滤。"""
    with get_pool().connection() as conn:
        rows = conn.execute(
            "SELECT date_key, payload FROM macro_data WHERE kind = %s "
            "AND (%s::text IS NULL OR date_key >= %s) "
            "AND (%s::text IS NULL OR date_key <= %s) ORDER BY date_key",
            (kind, start, start, end, end),
        ).fetchall()
    return [{"date_key": r[0], **(r[1] or {})} for r in rows]


def watermark_rows(
    code: str | None, dataset: str | None, limit: int, offset: int
) -> tuple[int, list[dict[str, Any]]]:
    where: list[str] = []
    params: list = []
    if code is not None:
        where.append("code = %s")
        params.append(code)
    if dataset is not None:
        where.append("dataset = %s")
        params.append(dataset)
    cond = ("WHERE " + " AND ".join(where)) if where else ""
    with get_pool().connection() as conn:
        total = conn.execute(
            f"SELECT count(*) FROM sync_watermark {cond}", params  # noqa: S608
        ).fetchone()[0]
        rows = conn.execute(
            f"SELECT code, dataset, first_date, last_date, last_synced_at "  # noqa: S608
            f"FROM sync_watermark {cond} ORDER BY code, dataset LIMIT %s OFFSET %s",
            params + [limit, offset],
        ).fetchall()
    return total, [
        {
            "code": r[0], "dataset": r[1], "first_date": r[2],
            "last_date": r[3], "last_synced_at": r[4],
        }
        for r in rows
    ]


def kline_gaps(code: str) -> dict[str, Any]:
    """日 K 缺口体检：水位覆盖区间内「是交易日但无 K 线行」的日期。

    缺口 = 停牌日（合法缺行）或真实缺数，需结合停牌信息人工判断；
    体检的价值在于缺口数量异常（如连续大段）时报警。
    """
    with get_pool().connection() as conn:
        wm = conn.execute(
            "SELECT first_date, last_date FROM sync_watermark "
            "WHERE code = %s AND dataset = 'k_d'",
            (code,),
        ).fetchone()
        if wm is None or wm[1] is None:
            return {"code": code, "first_date": None, "last_date": None,
                    "trading_days": 0, "missing": []}
        trading_days = conn.execute(
            "SELECT count(*) FROM trade_calendar "
            "WHERE is_trading_day AND calendar_date BETWEEN %s AND %s",
            (wm[0], wm[1]),
        ).fetchone()[0]
        rows = conn.execute(
            """
            SELECT calendar_date FROM trade_calendar
            WHERE is_trading_day AND calendar_date BETWEEN %s AND %s
            EXCEPT
            SELECT trade_date FROM kline WHERE code = %s AND frequency = 'd'
            ORDER BY 1
            """,
            (wm[0], wm[1], code),
        ).fetchall()
    return {
        "code": code, "first_date": wm[0], "last_date": wm[1],
        "trading_days": trading_days, "missing": [r[0] for r in rows],
    }


def financial_rows(
    codes: list[str], report_type: str, start: date | None, end: date | None
) -> dict[str, list[dict[str, Any]]]:
    with get_pool().connection() as conn:
        rows = conn.execute(
            "SELECT code, stat_date, pub_date, metrics FROM financial_report "
            "WHERE code = ANY(%s) AND report_type = %s "
            "AND (%s::date IS NULL OR stat_date >= %s) "
            "AND (%s::date IS NULL OR stat_date <= %s) "
            "ORDER BY code, stat_date",
            (codes, report_type, start, start, end, end),
        ).fetchall()
    out: dict[str, list[dict[str, Any]]] = {c: [] for c in codes}
    for r in rows:
        out[r[0]].append({"stat_date": r[1], "pub_date": r[2], "metrics": r[3]})
    return out


def dividend_rows(
    codes: list[str], year: int | None
) -> dict[str, list[dict[str, Any]]]:
    with get_pool().connection() as conn:
        rows = conn.execute(
            "SELECT code, plan_announce_date, year_type, operate_date, detail "
            "FROM dividend WHERE code = ANY(%s) "
            "AND (%s::int IS NULL OR EXTRACT(YEAR FROM plan_announce_date) = %s) "
            "ORDER BY code, plan_announce_date",
            (codes, year, year),
        ).fetchall()
    out: dict[str, list[dict[str, Any]]] = {c: [] for c in codes}
    for r in rows:
        out[r[0]].append({
            "plan_announce_date": r[1], "year_type": r[2],
            "operate_date": r[3], "detail": r[4],
        })
    return out
