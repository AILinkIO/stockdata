"""db/coverage.py 规则单测：覆盖设计文档 5.4 节规则表的每一行（正例 + 反例）。"""

from datetime import date, datetime, timedelta, timezone

import pytest

from db.coverage import (
    Decision,
    check_quarter,
    check_range,
    check_snapshot,
    quarter_disclosure_deadline,
    settled_boundary,
)
from db.models import DataWatermark

# 固定"现在"：2026-06-11（周四）12:00 UTC+8
CST = timezone(timedelta(hours=8))
NOW = datetime(2026, 6, 11, 12, 0, 0, tzinfo=CST)
TODAY = NOW.date()


def wm(first: date, last: date, fetched_ago_seconds: int = 0) -> DataWatermark:
    w = DataWatermark()
    w.code = "sh.600000"
    w.data_type = "k_d"
    w.first_date = first
    w.last_date = last
    w.last_fetched_at = NOW - timedelta(seconds=fetched_ago_seconds)
    return w


# ── check_range：通用规则 ──


def test_no_watermark_full_backfill():
    d = check_range(None, "k_d", date(2024, 1, 1), date(2024, 12, 31), NOW)
    assert not d.fresh
    assert d.fetch_ranges == [(date(1990, 12, 19), date(2024, 12, 31))]


def test_minute_backfill_start_respected():
    d = check_range(None, "k_30", date(2024, 1, 1), date(2024, 6, 1), NOW)
    # 回填起点来自 settings.minute_backfill_start（默认 2023-01-01）
    assert d.fetch_ranges[0][0] >= date(2023, 1, 1)


def test_covered_history_is_fresh_forever():
    w = wm(date(2020, 1, 1), date(2024, 12, 31), fetched_ago_seconds=10**7)  # 很久以前抓的
    d = check_range(w, "k_d", date(2024, 1, 1), date(2024, 12, 31), NOW)
    assert d.fresh  # 历史数据永久有效，与抓取时间无关


def test_tail_gap():
    w = wm(date(2020, 1, 1), date(2025, 12, 31))
    d = check_range(w, "k_d", date(2025, 1, 1), date(2026, 6, 10), NOW)
    assert d.fetch_ranges == [(date(2026, 1, 1), date(2026, 6, 10))]


def test_head_gap():
    w = wm(date(2020, 1, 1), date(2025, 12, 31))
    d = check_range(w, "k_d", date(2019, 1, 1), date(2020, 6, 1), NOW)
    assert d.fetch_ranges == [(date(2019, 1, 1), date(2019, 12, 31))]


def test_today_fresh_within_interval():
    w = wm(date(2020, 1, 1), TODAY, fetched_ago_seconds=60)  # 1 分钟前抓的
    d = check_range(w, "k_d", date(2026, 6, 1), TODAY, NOW)
    assert d.fresh  # 5 分钟内不重抓


def test_today_stale_after_interval():
    w = wm(date(2020, 1, 1), TODAY, fetched_ago_seconds=600)  # 10 分钟前
    d = check_range(w, "k_d", date(2026, 6, 1), TODAY, NOW)
    assert d.fetch_ranges == [(TODAY, TODAY)]  # 只刷新未定型部分


def test_tail_gap_and_stale_merge():
    w = wm(date(2020, 1, 1), TODAY - timedelta(days=3), fetched_ago_seconds=600)
    d = check_range(w, "k_d", date(2026, 6, 1), TODAY, NOW)
    # 尾部缺口 (6/9~6/11) 与刷新区 (6/11) 合并为一段
    assert d.fetch_ranges == [(TODAY - timedelta(days=2), TODAY)]


# ── 周/月线定型边界 ──


def test_weekly_settled_boundary_is_monday():
    # 2026-06-11 是周四 → 本周一 6/8，6/7（周日）及以前定型
    assert settled_boundary("k_w", TODAY) == date(2026, 6, 7)


def test_weekly_past_week_fresh_even_if_old_fetch():
    w = wm(date(2020, 1, 1), date(2026, 6, 5), fetched_ago_seconds=10**6)
    d = check_range(w, "k_w", date(2026, 5, 1), date(2026, 6, 5), NOW)
    assert d.fresh  # 上周五收的周线已定型


def test_weekly_current_week_stale():
    w = wm(date(2020, 1, 1), TODAY, fetched_ago_seconds=600)
    d = check_range(w, "k_w", date(2026, 5, 1), TODAY, NOW)
    assert d.fetch_ranges == [(date(2026, 6, 8), TODAY)]  # 本周一起刷新


def test_monthly_settled_boundary():
    assert settled_boundary("k_m", TODAY) == date(2026, 5, 31)  # 本月 1 日之前


# ── 季度财报 ──


def test_quarter_deadline_table():
    assert quarter_disclosure_deadline(2025, 1) == date(2025, 4, 30)
    assert quarter_disclosure_deadline(2025, 2) == date(2025, 8, 31)
    assert quarter_disclosure_deadline(2025, 3) == date(2025, 10, 31)
    assert quarter_disclosure_deadline(2025, 4) == date(2026, 4, 30)  # Q4 跨年


def test_quarter_never_fetched():
    d = check_quarter(False, None, 2024, 3, NOW)
    assert d.fetch_ranges == [(date(2024, 7, 1), date(2024, 9, 30))]


def test_quarter_settled_permanent():
    # 2024Q3 截止日 2024-10-31 已过且有数据：即使抓取很久，也永久有效
    assert check_quarter(True, NOW - timedelta(days=300), 2024, 3, NOW).fresh


def test_quarter_in_disclosure_window_stale():
    # 2026Q2（截止 8/31 未到）：有数据但 2 天没刷新 → 重抓（可能有修正/补披露）
    d = check_quarter(True, NOW - timedelta(days=2), 2026, 2, NOW)
    assert d.fetch_ranges == [(date(2026, 4, 1), date(2026, 6, 30))]


def test_quarter_in_disclosure_window_fresh():
    assert check_quarter(True, NOW - timedelta(hours=1), 2026, 2, NOW).fresh


def test_quarter_hole_not_masked_by_other_quarters():
    # 设计缺陷回归测试：抓过 2024Q3 与 2026Q2 不应让从未抓过的 2026Q1 被判已覆盖
    d = check_quarter(False, None, 2026, 1, NOW)
    assert not d.fresh


def test_quarter_empty_after_deadline_is_permanent():
    # 截止日后查过且确实没有（如退市/未上市期间）：永久空结果，不再重查
    assert check_quarter(False, NOW - timedelta(days=10), 2026, 1, NOW).fresh


def test_quarter_empty_in_window_rechecks_daily():
    # 披露期内查过没有：1 天内不重查，超过则再查（可能刚披露）
    assert check_quarter(False, NOW - timedelta(hours=2), 2026, 2, NOW).fresh
    d = check_quarter(False, NOW - timedelta(days=2), 2026, 2, NOW)
    assert not d.fresh


# ── 宏观沉淀期 ──


def test_macro_settled_after_60_days():
    w = wm(date(2020, 1, 1), TODAY - timedelta(days=90), fetched_ago_seconds=10**7)
    d = check_range(w, "money_supply_month", date(2025, 1, 1), TODAY - timedelta(days=90), NOW)
    assert d.fresh


def test_macro_recent_stale_weekly():
    w = wm(date(2020, 1, 1), TODAY, fetched_ago_seconds=8 * 86400)  # 8 天前
    d = check_range(w, "deposit_rate", date(2026, 1, 1), TODAY, NOW)
    assert not d.fresh
    # 刷新区从沉淀期边界起
    assert d.fetch_ranges[0][0] == TODAY - timedelta(days=59)


def test_macro_recent_fresh_within_week():
    w = wm(date(2020, 1, 1), TODAY, fetched_ago_seconds=86400)  # 1 天前
    d = check_range(w, "deposit_rate", date(2026, 1, 1), TODAY, NOW)
    assert d.fresh


# ── 交易日历（可请求未来） ──


def test_calendar_future_not_clamped():
    # 新鲜水位：只补尾部缺口，且不被 clamp 到今天
    w = wm(date(2024, 1, 1), date(2026, 6, 30), fetched_ago_seconds=3600)
    d = check_range(w, "trade_calendar", date(2026, 1, 1), date(2026, 12, 31), NOW)
    assert d.fetch_ranges == [(date(2026, 7, 1), date(2026, 12, 31))]


def test_calendar_stale_refreshes_unsettled_and_merges_tail():
    # 过期水位：今天起的未定型区段（临时调整可能）与尾部缺口合并重抓
    w = wm(date(2024, 1, 1), date(2026, 6, 30), fetched_ago_seconds=2 * 86400)
    d = check_range(w, "trade_calendar", date(2026, 1, 1), date(2026, 12, 31), NOW)
    assert d.fetch_ranges == [(TODAY, date(2026, 12, 31))]


# ── 快照类 ──


def test_snapshot_missing():
    d = check_snapshot(None, "stock_list", TODAY, has_rows=False, now=NOW)
    assert d.fetch_ranges == [(TODAY, TODAY)]


def test_snapshot_historical_permanent():
    d = check_snapshot(None, "stock_list", TODAY - timedelta(days=5), has_rows=True, now=NOW)
    assert d.fresh  # 历史快照不变，无需水位


def test_snapshot_today_stale():
    w = wm(TODAY, TODAY, fetched_ago_seconds=2 * 86400)
    d = check_snapshot(w, "stock_list", TODAY, has_rows=True, now=NOW)
    assert d.fetch_ranges == [(TODAY, TODAY)]


def test_snapshot_today_fresh():
    w = wm(TODAY, TODAY, fetched_ago_seconds=3600)
    d = check_snapshot(w, "stock_list", TODAY, has_rows=True, now=NOW)
    assert d.fresh
