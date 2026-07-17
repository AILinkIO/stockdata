"""切片规划基础件：结算边界、区间切片、季度/披露截止日运算、计划上下文。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import TYPE_CHECKING

import psycopg

if TYPE_CHECKING:
    from stockdata.config import Settings
    from stockdata.provider.interface import Provider

    from .watermark import Watermark

# A 股最早数据日（baostock 数据起点）
A_SHARE_EPOCH = date(1990, 12, 19)


@dataclass
class Slice:
    """一次可独立提交的抓取单元（= 断点续传的最小粒度）。

    empty_advance_to：空结果时允许把水位推进到的「已结算边界」；None 表示
    空结果不推进（未结算尾部，下次重查）。
    """

    start: date | None
    end: date | None
    label: str
    empty_advance_to: date | None = None
    meta: dict = field(default_factory=dict)


@dataclass
class SliceResult:
    rows: int
    actual_first: date | None = None
    actual_last: date | None = None


@dataclass
class PlanContext:
    conn: psycopg.Connection
    provider: "Provider"
    settings: "Settings"
    today: date
    code: str = ""                      # 市场级数据集为 ''
    ipo_date: date | None = None
    wm: "Watermark | None" = None

    def resume_start(self, floor: date) -> date:
        """断点：水位之后一天；无水位从 floor 起。"""
        if self.wm is not None and self.wm.last_date is not None:
            return max(self.wm.last_date + timedelta(days=1), floor)
        return floor

    def is_fresh(self, hours: int) -> bool:
        """last_synced_at 距今不足 hours 小时（stale 门控，避免每次 run 重查未结算数据）。"""
        if self.wm is None or self.wm.last_synced_at is None:
            return False
        from datetime import UTC, datetime

        return (datetime.now(UTC) - self.wm.last_synced_at).total_seconds() < hours * 3600


def settled_daily(today: date) -> date:
    """日线/分钟线结算边界：昨天（今天的数据盘中不稳定）。"""
    return today - timedelta(days=1)


def settled_weekly(today: date) -> date:
    """周线结算边界：上一个已收盘的周五（周线 bar 日期为当周最后交易日，通常周五；
    今天即使是周五也未收盘结算，取上一周五）。"""
    days_since_friday = (today.isoweekday() - 5) % 7 or 7
    return today - timedelta(days=days_since_friday)


def slice_range(start: date, end: date, span_days: int, label: str) -> list[Slice]:
    """[start, end] 按 span_days 切片；每片空结果可推进到片尾（片尾 ≤ 结算边界由调用方保证）。"""
    slices: list[Slice] = []
    cur = start
    while cur <= end:
        piece_end = min(cur + timedelta(days=span_days - 1), end)
        slices.append(
            Slice(cur, piece_end, f"{label} {cur}~{piece_end}", empty_advance_to=piece_end)
        )
        cur = piece_end + timedelta(days=1)
    return slices


def quarter_end(year: int, quarter: int) -> date:
    month = quarter * 3
    if month == 3:
        return date(year, 3, 31)
    if month == 6:
        return date(year, 6, 30)
    if month == 9:
        return date(year, 9, 30)
    return date(year, 12, 31)


def disclosure_deadline(year: int, quarter: int) -> date:
    """A 股定期报告披露截止日：Q1→4/30，Q2→8/31，Q3→10/31，Q4→次年 4/30。"""
    if quarter == 1:
        return date(year, 4, 30)
    if quarter == 2:
        return date(year, 8, 31)
    if quarter == 3:
        return date(year, 10, 31)
    return date(year + 1, 4, 30)


def quarters_between(floor: date, today: date) -> list[tuple[int, int]]:
    """报告期结束日 ∈ [floor, today) 的所有 (year, quarter)，升序。"""
    result = []
    year, quarter = floor.year, (floor.month - 1) // 3 + 1
    while True:
        qe = quarter_end(year, quarter)
        if qe >= today:
            break
        if qe >= floor:
            result.append((year, quarter))
        quarter += 1
        if quarter == 5:
            year, quarter = year + 1, 1
    return result


def last_trading_day(conn: psycopg.Connection, today: date) -> date | None:
    """交易日历中 ≤ today 的最近交易日（日历未同步时返回 None）。"""
    row = conn.execute(
        "SELECT max(calendar_date) FROM trade_calendar "
        "WHERE is_trading_day AND calendar_date <= %s",
        (today,),
    ).fetchone()
    return row[0] if row else None
