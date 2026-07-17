"""baostock 字符串 DataFrame → 类型化行 → PG upsert。

baostock 返回值全部是字符串（空串=缺失），这里统一做类型转换与 ON CONFLICT upsert。
所有 upsert 幂等：断点续传重跑同一切片不产生重复/冲突。
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg

logger = logging.getLogger(__name__)

_SH_TZ = ZoneInfo("Asia/Shanghai")


def _num(s: str | None) -> float | None:
    if s is None or s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _int(s: str | None) -> int | None:
    v = _num(s)
    return int(v) if v is not None else None


def _date(s: str | None) -> date | None:
    if not s:
        return None
    return date.fromisoformat(s)


def _bool01(s: str | None) -> bool | None:
    if s is None or s == "":
        return None
    return s == "1"


def _bar_time(s: str) -> datetime:
    """baostock 分钟线 time 字段：'YYYYMMDDHHMMSS000' → tz-aware datetime（bar 结束时刻）。"""
    return datetime.strptime(s[:14], "%Y%m%d%H%M%S").replace(tzinfo=_SH_TZ)


def _row_get(row: pd.Series, key: str) -> str:
    v = row.get(key)
    return "" if v is None else str(v)


# ── K 线 ──


def upsert_kline(conn: psycopg.Connection, frequency: str, df: pd.DataFrame) -> int:
    """日/周 K 线 upsert。返回写入行数；返回值 0 表示 df 为空。"""
    if df.empty:
        return 0
    daily = frequency == "d"
    rows = []
    for _, r in df.iterrows():
        rows.append((
            r["code"], frequency, _date(r["date"]),
            _num(r["open"]), _num(r["high"]), _num(r["low"]), _num(r["close"]),
            _int(r["volume"]), _num(r["amount"]), _num(r["turn"]), _num(r["pctChg"]),
            _num(r["preclose"]) if daily else None,
            _int(r["tradestatus"]) if daily else None,
            _bool01(r["isST"]) if daily else None,
            _num(r["peTTM"]) if daily else None,
            _num(r["pbMRQ"]) if daily else None,
            _num(r["psTTM"]) if daily else None,
            _num(r["pcfNcfTTM"]) if daily else None,
        ))
    conn.cursor().executemany(
        """
        INSERT INTO kline (code, frequency, trade_date, open, high, low, close,
                           volume, amount, turn, pct_chg, preclose, trade_status,
                           is_st, pe_ttm, pb_mrq, ps_ttm, pcf_ncf_ttm)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (code, frequency, trade_date) DO UPDATE SET
            open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
            close=EXCLUDED.close, volume=EXCLUDED.volume, amount=EXCLUDED.amount,
            turn=EXCLUDED.turn, pct_chg=EXCLUDED.pct_chg, preclose=EXCLUDED.preclose,
            trade_status=EXCLUDED.trade_status, is_st=EXCLUDED.is_st,
            pe_ttm=EXCLUDED.pe_ttm, pb_mrq=EXCLUDED.pb_mrq, ps_ttm=EXCLUDED.ps_ttm,
            pcf_ncf_ttm=EXCLUDED.pcf_ncf_ttm
        """,
        rows,
    )
    return len(rows)


def upsert_kline_minute(conn: psycopg.Connection, frequency: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = [
        (
            r["code"], frequency, _bar_time(r["time"]),
            _num(r["open"]), _num(r["high"]), _num(r["low"]), _num(r["close"]),
            _int(r["volume"]), _num(r["amount"]),
        )
        for _, r in df.iterrows()
    ]
    conn.cursor().executemany(
        """
        INSERT INTO kline_minute (code, frequency, bar_time, open, high, low, close,
                                  volume, amount)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (code, frequency, bar_time) DO UPDATE SET
            open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
            close=EXCLUDED.close, volume=EXCLUDED.volume, amount=EXCLUDED.amount
        """,
        rows,
    )
    return len(rows)


# ── 复权因子 / 分红 / 财报 ──


def upsert_adjust_factor(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = [
        (
            r["code"], _date(r["dividOperateDate"]),
            _num(r["foreAdjustFactor"]), _num(r["backAdjustFactor"]), _num(r["adjustFactor"]),
        )
        for _, r in df.iterrows()
    ]
    conn.cursor().executemany(
        """
        INSERT INTO adjust_factor (code, divid_operate_date, fore_adjust_factor,
                                   back_adjust_factor, adjust_factor)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (code, divid_operate_date) DO UPDATE SET
            fore_adjust_factor=EXCLUDED.fore_adjust_factor,
            back_adjust_factor=EXCLUDED.back_adjust_factor,
            adjust_factor=EXCLUDED.adjust_factor
        """,
        rows,
    )
    return len(rows)


def upsert_dividend(conn: psycopg.Connection, year_type: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = []
    for _, r in df.iterrows():
        plan_date = (
            _date(_row_get(r, "dividPlanAnnounceDate"))
            or _date(_row_get(r, "dividPlanDate"))
            or _date(_row_get(r, "dividOperateDate"))
        )
        if plan_date is None:  # 无任何可用日期，无法定位主键
            continue
        rows.append((
            r["code"], plan_date, year_type,
            _date(_row_get(r, "dividOperateDate")),
            json.dumps(r.to_dict(), ensure_ascii=False),
        ))
    if not rows:
        return 0
    conn.cursor().executemany(
        """
        INSERT INTO dividend (code, plan_announce_date, year_type, operate_date, detail)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (code, plan_announce_date, year_type) DO UPDATE SET
            operate_date=EXCLUDED.operate_date, detail=EXCLUDED.detail
        """,
        rows,
    )
    return len(rows)


def upsert_financial(
    conn: psycopg.Connection, code: str, report_type: str, metrics: dict
) -> int:
    """单季度单类财报 upsert。metrics 为 baostock 原始行 dict（含 statDate/pubDate）。"""
    stat_date = _date(str(metrics.get("statDate", "")) or None)
    if stat_date is None:
        return 0
    conn.execute(
        """
        INSERT INTO financial_report (code, report_type, stat_date, pub_date, metrics)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (code, report_type, stat_date) DO UPDATE SET
            pub_date=EXCLUDED.pub_date, metrics=EXCLUDED.metrics
        """,
        (
            code, report_type, stat_date,
            _date(str(metrics.get("pubDate", "")) or None),
            json.dumps(metrics, ensure_ascii=False),
        ),
    )
    return 1


def upsert_report_events(
    conn: psycopg.Connection, report_type: str, df: pd.DataFrame,
    stat_field: str, pub_field: str,
) -> int:
    """业绩快报/预告：整行进 metrics，stat/pub 日期来自指定字段。"""
    if df.empty:
        return 0
    n = 0
    for _, r in df.iterrows():
        stat_date = _date(_row_get(r, stat_field))
        if stat_date is None:
            continue
        conn.execute(
            """
            INSERT INTO financial_report (code, report_type, stat_date, pub_date, metrics)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (code, report_type, stat_date) DO UPDATE SET
                pub_date=EXCLUDED.pub_date, metrics=EXCLUDED.metrics
            """,
            (
                r["code"], report_type, stat_date,
                _date(_row_get(r, pub_field)),
                json.dumps(r.to_dict(), ensure_ascii=False),
            ),
        )
        n += 1
    return n


# ── 市场级 ──


def upsert_trade_calendar(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = [(_date(r["calendar_date"]), r["is_trading_day"] == "1") for _, r in df.iterrows()]
    conn.cursor().executemany(
        """
        INSERT INTO trade_calendar (calendar_date, is_trading_day)
        VALUES (%s,%s)
        ON CONFLICT (calendar_date) DO UPDATE SET is_trading_day=EXCLUDED.is_trading_day
        """,
        rows,
    )
    return len(rows)


def upsert_security(conn: psycopg.Connection, df: pd.DataFrame) -> int:
    """query_stock_basic 全量 → security。"""
    if df.empty:
        return 0
    rows = [
        (
            r["code"], _row_get(r, "code_name"),
            _date(_row_get(r, "ipoDate")), _date(_row_get(r, "outDate")),
            _int(_row_get(r, "type")), _int(_row_get(r, "status")),
        )
        for _, r in df.iterrows()
    ]
    conn.cursor().executemany(
        """
        INSERT INTO security (code, code_name, ipo_date, out_date, type, status, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,now())
        ON CONFLICT (code) DO UPDATE SET
            code_name=EXCLUDED.code_name, ipo_date=EXCLUDED.ipo_date,
            out_date=EXCLUDED.out_date, type=EXCLUDED.type, status=EXCLUDED.status,
            updated_at=now()
        """,
        rows,
    )
    return len(rows)


def upsert_stock_list(conn: psycopg.Connection, snap_date: date, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = [
        (snap_date, r["code"], _row_get(r, "code_name"), _int(_row_get(r, "tradeStatus")))
        for _, r in df.iterrows()
    ]
    conn.cursor().executemany(
        """
        INSERT INTO stock_list_snapshot (snap_date, code, code_name, trade_status)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (snap_date, code) DO UPDATE SET
            code_name=EXCLUDED.code_name, trade_status=EXCLUDED.trade_status
        """,
        rows,
    )
    return len(rows)


def upsert_industry(conn: psycopg.Connection, snap_date: date, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = [
        (
            snap_date, r["code"],
            _row_get(r, "industry") or None, _row_get(r, "industryClassification") or None,
        )
        for _, r in df.iterrows()
    ]
    conn.cursor().executemany(
        """
        INSERT INTO stock_industry (snap_date, code, industry, industry_classification)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (snap_date, code) DO UPDATE SET
            industry=EXCLUDED.industry,
            industry_classification=EXCLUDED.industry_classification
        """,
        rows,
    )
    return len(rows)


def upsert_index_constituent(
    conn: psycopg.Connection, index_code: str, snap_date: date, df: pd.DataFrame
) -> int:
    if df.empty:
        return 0
    rows = [
        (index_code, snap_date, r["code"], _row_get(r, "code_name") or None)
        for _, r in df.iterrows()
    ]
    conn.cursor().executemany(
        """
        INSERT INTO index_constituent (index_code, snap_date, code, code_name)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (index_code, snap_date, code) DO UPDATE SET code_name=EXCLUDED.code_name
        """,
        rows,
    )
    return len(rows)


# 宏观各 kind 的 date_key 提取
def _macro_date_key(kind: str, row: dict) -> str | None:
    if kind in ("deposit_rate", "loan_rate"):
        return row.get("pubDate") or None
    if kind == "rrr":
        pub = row.get("pubDate") or ""
        eff = row.get("effectiveDate") or ""
        return f"{pub}|{eff}" if pub or eff else None
    if kind == "money_supply_month":
        y, m = row.get("statYear"), row.get("statMonth")
        return f"{y}-{int(m):02d}" if y and m else None
    if kind == "money_supply_year":
        return row.get("statYear") or None
    raise ValueError(f"未知宏观 kind: {kind}")


def upsert_macro(conn: psycopg.Connection, kind: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    n = 0
    for _, r in df.iterrows():
        row = r.to_dict()
        date_key = _macro_date_key(kind, row)
        if not date_key:
            continue
        conn.execute(
            """
            INSERT INTO macro_data (kind, date_key, payload)
            VALUES (%s,%s,%s)
            ON CONFLICT (kind, date_key) DO UPDATE SET payload=EXCLUDED.payload
            """,
            (kind, date_key, json.dumps(row, ensure_ascii=False)),
        )
        n += 1
    return n
