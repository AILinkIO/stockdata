"""数据集注册表：每个数据集一个 handler（plan 切片 + run_slice 抓取入库）。

引擎对所有 handler 统一执行：逐片 fetch → 写库 → 推水位（同一事务），
NoDataFoundError 在 run_slice 内转为 rows=0（空结果由 Slice.empty_advance_to 决定水位）。
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from enum import StrEnum

from stockdata.provider.interface import NoDataFoundError

from . import writers
from .planner import (
    A_SHARE_EPOCH,
    PlanContext,
    Slice,
    SliceResult,
    disclosure_deadline,
    last_trading_day,
    quarter_end,
    quarters_between,
    settled_daily,
    settled_weekly,
    slice_range,
)

logger = logging.getLogger(__name__)


class Dataset(StrEnum):
    # 市场级（code=''）
    TRADE_CALENDAR = "trade_calendar"
    SECURITY = "security"
    STOCK_LIST = "stock_list"
    INDUSTRY = "industry"
    INDEX_SZ50 = "index_sz50"
    INDEX_HS300 = "index_hs300"
    INDEX_ZZ500 = "index_zz500"
    MACRO_DEPOSIT_RATE = "macro_deposit_rate"
    MACRO_LOAN_RATE = "macro_loan_rate"
    MACRO_RRR = "macro_rrr"
    MACRO_MONEY_SUPPLY_MONTH = "macro_money_supply_month"
    MACRO_MONEY_SUPPLY_YEAR = "macro_money_supply_year"
    # 按码
    STOCK_BASIC = "stock_basic"
    K_D = "k_d"
    K_W = "k_w"
    K_5 = "k_5"
    K_30 = "k_30"
    ADJUST_FACTOR = "adjust_factor"
    DIVIDEND = "dividend"
    FINANCIAL = "financial"
    PERFORMANCE_EXPRESS = "performance_express"
    FORECAST = "forecast"


def _df_date_span(df, col: str = "date") -> tuple[date | None, date | None]:
    if df.empty or col not in df.columns:
        return None, None
    vals = [v for v in df[col] if v]
    if not vals:
        return None, None
    return date.fromisoformat(min(vals)), date.fromisoformat(max(vals))


def _fetch(fn, *args, **kwargs):
    """NoDataFoundError → None（合法空结果）。"""
    try:
        return fn(*args, **kwargs)
    except NoDataFoundError:
        return None


# ── 市场级 handlers ──


class TradeCalendarHandler:
    dataset = Dataset.TRADE_CALENDAR

    def plan(self, ctx: PlanContext) -> list[Slice]:
        end = date(ctx.today.year, 12, 31)  # 日历可取到年底（含未来节假日安排）
        start = ctx.resume_start(A_SHARE_EPOCH)
        if start > end:
            return []
        return [Slice(start, end, f"交易日历 {start}~{end}", empty_advance_to=None)]

    def run_slice(self, ctx: PlanContext, sl: Slice) -> SliceResult:
        df = _fetch(ctx.provider.query_trade_dates, sl.start.isoformat(), sl.end.isoformat())
        if df is None:
            return SliceResult(0)
        rows = writers.upsert_trade_calendar(ctx.conn, df)
        first, last = _df_date_span(df, "calendar_date")
        return SliceResult(rows, first, last)


class SecurityHandler:
    """全市场证券表（query_stock_basic 全量），stale 门控。"""

    dataset = Dataset.SECURITY

    def plan(self, ctx: PlanContext) -> list[Slice]:
        if ctx.is_fresh(ctx.settings.stale_after_hours):
            return []
        return [Slice(None, None, "证券列表（全市场）", empty_advance_to=None)]

    def run_slice(self, ctx: PlanContext, sl: Slice) -> SliceResult:
        df = _fetch(ctx.provider.query_stock_basic)
        if df is None:
            return SliceResult(0)
        rows = writers.upsert_security(ctx.conn, df)
        return SliceResult(rows, None, ctx.today)


class StockListHandler:
    dataset = Dataset.STOCK_LIST

    def plan(self, ctx: PlanContext) -> list[Slice]:
        snap = last_trading_day(ctx.conn, ctx.today) or settled_daily(ctx.today)
        if ctx.wm is not None and ctx.wm.last_date is not None and ctx.wm.last_date >= snap:
            return []
        return [Slice(snap, snap, f"股票列表快照 {snap}", empty_advance_to=snap,
                      meta={"snap": snap})]

    def run_slice(self, ctx: PlanContext, sl: Slice) -> SliceResult:
        snap = sl.meta["snap"]
        df = _fetch(ctx.provider.query_all_stock, snap.isoformat())
        if df is None:
            return SliceResult(0)
        rows = writers.upsert_stock_list(ctx.conn, snap, df)
        return SliceResult(rows, snap, snap)


class _SnapshotHandler:
    """行业分类 / 指数成分：按 snapshot_refresh_days 周期重抓最近交易日快照。"""

    def plan(self, ctx: PlanContext) -> list[Slice]:
        if ctx.is_fresh(ctx.settings.snapshot_refresh_days * 24):
            return []
        snap = last_trading_day(ctx.conn, ctx.today) or settled_daily(ctx.today)
        return [Slice(snap, snap, f"{self.dataset} 快照 {snap}", empty_advance_to=snap,
                      meta={"snap": snap})]


class IndustryHandler(_SnapshotHandler):
    dataset = Dataset.INDUSTRY

    def run_slice(self, ctx: PlanContext, sl: Slice) -> SliceResult:
        snap = sl.meta["snap"]
        df = _fetch(ctx.provider.query_industry, snap.isoformat())
        if df is None:
            return SliceResult(0)
        rows = writers.upsert_industry(ctx.conn, snap, df)
        return SliceResult(rows, snap, snap)


class IndexConstituentHandler(_SnapshotHandler):
    def __init__(self, dataset: Dataset, index_code: str) -> None:
        self.dataset = dataset
        self.index_code = index_code

    def run_slice(self, ctx: PlanContext, sl: Slice) -> SliceResult:
        snap = sl.meta["snap"]
        df = _fetch(ctx.provider.query_index_constituent, self.index_code, snap.isoformat())
        if df is None:
            return SliceResult(0)
        rows = writers.upsert_index_constituent(ctx.conn, self.index_code, snap, df)
        return SliceResult(rows, snap, snap)


class MacroHandler:
    """宏观数据：range 查询；结算边界 today-60d（数据发布有滞后）。"""

    _FLOOR = date(1990, 1, 1)

    def __init__(self, dataset: Dataset, kind: str) -> None:
        self.dataset = dataset
        self.kind = kind

    def _fmt(self, d: date) -> str:
        if self.kind == "money_supply_month":
            return d.strftime("%Y-%m")
        if self.kind == "money_supply_year":
            return d.strftime("%Y")
        return d.isoformat()

    def plan(self, ctx: PlanContext) -> list[Slice]:
        if ctx.is_fresh(ctx.settings.stale_after_hours):
            return []
        start = ctx.resume_start(self._FLOOR)
        end = ctx.today
        if start > end:
            return []
        settled = ctx.today - timedelta(days=60)
        return [Slice(start, end, f"宏观 {self.kind} {start}~{end}",
                      empty_advance_to=settled if settled >= start else None)]

    def _actual_last(self, df) -> date | None:
        cols = df.columns
        if "pubDate" in cols:
            vals = [v for v in df["pubDate"] if v]
            return date.fromisoformat(max(vals)) if vals else None
        if "statMonth" in cols:
            pairs = [(int(y), int(m)) for y, m in zip(df["statYear"], df["statMonth"]) if y and m]
            if not pairs:
                return None
            y, m = max(pairs)
            return date(y, m, 1)
        if "statYear" in cols:
            years = [int(y) for y in df["statYear"] if y]
            return date(max(years), 1, 1) if years else None
        return None

    def run_slice(self, ctx: PlanContext, sl: Slice) -> SliceResult:
        df = _fetch(ctx.provider.query_macro, self.kind, self._fmt(sl.start), self._fmt(sl.end))
        if df is None:
            return SliceResult(0)
        rows = writers.upsert_macro(ctx.conn, self.kind, df)
        return SliceResult(rows, sl.start, self._actual_last(df))


# ── 按码 handlers ──


class StockBasicHandler:
    dataset = Dataset.STOCK_BASIC

    def plan(self, ctx: PlanContext) -> list[Slice]:
        if ctx.is_fresh(ctx.settings.stale_after_hours):
            return []
        return [Slice(None, None, f"基本信息 {ctx.code}", empty_advance_to=None)]

    def run_slice(self, ctx: PlanContext, sl: Slice) -> SliceResult:
        df = _fetch(ctx.provider.query_stock_basic, ctx.code)
        if df is None:
            return SliceResult(0)
        rows = writers.upsert_security(ctx.conn, df)
        return SliceResult(rows, None, ctx.today)


class KlineHandler:
    """日/周 K 线：水位缺口切片，空片（长停牌/未上市区间）推进到片尾。"""

    def __init__(self, dataset: Dataset, frequency: str) -> None:
        self.dataset = dataset
        self.frequency = frequency

    def plan(self, ctx: PlanContext) -> list[Slice]:
        floor = max(ctx.ipo_date or A_SHARE_EPOCH, A_SHARE_EPOCH)
        end = (
            settled_weekly(ctx.today) if self.frequency == "w" else settled_daily(ctx.today)
        )
        start = ctx.resume_start(floor)
        if start > end:
            return []
        span = ctx.settings.kline_slice_days
        return slice_range(start, end, span, f"K线{self.frequency} {ctx.code}")

    def run_slice(self, ctx: PlanContext, sl: Slice) -> SliceResult:
        df = _fetch(
            ctx.provider.query_k_data,
            ctx.code, sl.start.isoformat(), sl.end.isoformat(), self.frequency,
        )
        if df is None:
            return SliceResult(0)
        rows = writers.upsert_kline(ctx.conn, self.frequency, df)
        first, last = _df_date_span(df)
        return SliceResult(rows, first, last)


class KlineMinuteHandler:
    def __init__(self, dataset: Dataset, frequency: str) -> None:
        self.dataset = dataset
        self.frequency = frequency

    def plan(self, ctx: PlanContext) -> list[Slice]:
        floor = max(ctx.ipo_date or A_SHARE_EPOCH, ctx.settings.minute_backfill_floor)
        end = settled_daily(ctx.today)
        start = ctx.resume_start(floor)
        if start > end:
            return []
        span = ctx.settings.minute_slice_days
        return slice_range(start, end, span, f"K线{self.frequency}分 {ctx.code}")

    def run_slice(self, ctx: PlanContext, sl: Slice) -> SliceResult:
        df = _fetch(
            ctx.provider.query_k_data,
            ctx.code, sl.start.isoformat(), sl.end.isoformat(), self.frequency,
        )
        if df is None:
            return SliceResult(0)
        rows = writers.upsert_kline_minute(ctx.conn, self.frequency, df)
        first, last = _df_date_span(df)
        return SliceResult(rows, first, last)


class AdjustFactorHandler:
    """复权因子：事件驱动全量重抓——分红出现比因子表更新的除权日即重抓，
    另按 snapshot_refresh_days 周期兜底。前复权因子依赖全历史，必须整段重抓。"""

    dataset = Dataset.ADJUST_FACTOR

    def plan(self, ctx: PlanContext) -> list[Slice]:
        div_max = ctx.conn.execute(
            "SELECT max(operate_date) FROM dividend WHERE code = %s", (ctx.code,)
        ).fetchone()[0]
        af_max = ctx.conn.execute(
            "SELECT max(divid_operate_date) FROM adjust_factor WHERE code = %s", (ctx.code,)
        ).fetchone()[0]
        event = div_max is not None and (af_max is None or div_max > af_max)
        if not event and ctx.is_fresh(ctx.settings.snapshot_refresh_days * 24):
            return []
        end = settled_daily(ctx.today)
        return [Slice(A_SHARE_EPOCH, end, f"复权因子 {ctx.code}（全量）",
                      empty_advance_to=end)]

    def run_slice(self, ctx: PlanContext, sl: Slice) -> SliceResult:
        df = _fetch(
            ctx.provider.query_adjust_factor,
            ctx.code, sl.start.isoformat(), sl.end.isoformat(),
        )
        if df is None:
            return SliceResult(0)
        rows = writers.upsert_adjust_factor(ctx.conn, df)
        first, last = _df_date_span(df, "dividOperateDate")
        return SliceResult(rows, first, last)


class DividendHandler:
    """分红：按年切片（report+operate 两种 yearType 各一次调用）。

    已结算年（< 当年）完成后水位推进到该年 12-31；当年数据每次 run 重查
    （stale 门控 20h），水位只随实际公告日推进，绝不虚报到年底。
    """

    dataset = Dataset.DIVIDEND

    def plan(self, ctx: PlanContext) -> list[Slice]:
        floor_year = (ctx.ipo_date or A_SHARE_EPOCH).year
        covered_until = ctx.wm.last_date if ctx.wm and ctx.wm.last_date else None
        pending: list[Slice] = []
        for year in range(floor_year, ctx.today.year + 1):
            year_settled = year < ctx.today.year
            if covered_until is not None and covered_until >= quarter_end(year, 4):
                continue  # 该年已覆盖
            pending.append(Slice(
                date(year, 1, 1), date(year, 12, 31), f"分红 {ctx.code} {year}",
                empty_advance_to=date(year, 12, 31) if year_settled else None,
                meta={"year": year},
            ))
        # 只剩当年未结算切片且刚查过 → 本轮跳过
        if pending and all(s.empty_advance_to is None for s in pending) \
                and ctx.is_fresh(ctx.settings.stale_after_hours):
            return []
        return pending

    def run_slice(self, ctx: PlanContext, sl: Slice) -> SliceResult:
        year = str(sl.meta["year"])
        rows = 0
        last: date | None = None
        for year_type in ("report", "operate"):
            df = _fetch(ctx.provider.query_dividend, ctx.code, year, year_type)
            if df is None:
                continue
            rows += writers.upsert_dividend(ctx.conn, year_type, df)
            _, d_last = _df_date_span(df, "dividPlanAnnounceDate")
            if d_last is not None:
                last = max(last, d_last) if last else d_last
        return SliceResult(rows, sl.start if rows else None,
                           (sl.end if sl.empty_advance_to else last) if rows else None)


class FinancialHandler:
    """六类季报：按 (年,季) 切片，6 次调用/片。

    披露截止日已过的季度空结果可推进（记「查过、无数据」）；未到截止日的季度
    只在拿到数据时推进，且受 stale 门控避免每 run 重查。
    """

    dataset = Dataset.FINANCIAL

    def plan(self, ctx: PlanContext) -> list[Slice]:
        floor = max(ctx.ipo_date or A_SHARE_EPOCH, ctx.settings.financial_backfill_floor)
        covered = ctx.wm.last_date if ctx.wm and ctx.wm.last_date else None
        pending: list[Slice] = []
        for year, quarter in quarters_between(floor, ctx.today):
            qe = quarter_end(year, quarter)
            if covered is not None and covered >= qe:
                continue
            settled = ctx.today > disclosure_deadline(year, quarter)
            pending.append(Slice(
                date(year, 3 * quarter - 2, 1), qe,
                f"财报 {ctx.code} {year}Q{quarter}",
                empty_advance_to=qe if settled else None,
                meta={"year": year, "quarter": quarter},
            ))
        if pending and all(s.empty_advance_to is None for s in pending) \
                and ctx.is_fresh(ctx.settings.stale_after_hours):
            return []
        return pending

    def run_slice(self, ctx: PlanContext, sl: Slice) -> SliceResult:
        result = ctx.provider.query_fina_quarter(
            ctx.code, str(sl.meta["year"]), sl.meta["quarter"]
        )
        rows = 0
        for report_type, metrics in result.items():
            rows += writers.upsert_financial(ctx.conn, ctx.code, report_type, metrics)
        qe = quarter_end(sl.meta["year"], sl.meta["quarter"])
        return SliceResult(rows, sl.start if rows else None, qe if rows else None)


class ReportEventHandler:
    """业绩快报/预告：稀疏事件流，按披露日区间增量查，留一周未结算尾部重查。"""

    def __init__(self, dataset: Dataset, query_name: str, stat_field: str, pub_field: str):
        self.dataset = dataset
        self.query_name = query_name
        self.stat_field = stat_field
        self.pub_field = pub_field

    def plan(self, ctx: PlanContext) -> list[Slice]:
        if ctx.is_fresh(ctx.settings.stale_after_hours):
            return []
        floor = max(ctx.ipo_date or A_SHARE_EPOCH, ctx.settings.financial_backfill_floor)
        start = ctx.resume_start(floor)
        end = ctx.today
        if start > end:
            return []
        settled = ctx.today - timedelta(days=7)
        return [Slice(start, end, f"{self.dataset} {ctx.code} {start}~{end}",
                      empty_advance_to=settled if settled >= start else None)]

    def run_slice(self, ctx: PlanContext, sl: Slice) -> SliceResult:
        fn = getattr(ctx.provider, self.query_name)
        df = _fetch(fn, ctx.code, sl.start.isoformat(), sl.end.isoformat())
        if df is None:
            return SliceResult(0)
        rows = writers.upsert_report_events(
            ctx.conn, self.dataset.value, df, self.stat_field, self.pub_field
        )
        vals = [v for v in df[self.pub_field] if v] if self.pub_field in df.columns else []
        last = date.fromisoformat(max(vals)) if vals else None
        return SliceResult(rows, sl.start, last)


# ── 注册表（顺序即执行顺序）──

MARKET_HANDLERS = [
    TradeCalendarHandler(),
    SecurityHandler(),
    StockListHandler(),
    IndustryHandler(),
    IndexConstituentHandler(Dataset.INDEX_SZ50, "sz50"),
    IndexConstituentHandler(Dataset.INDEX_HS300, "hs300"),
    IndexConstituentHandler(Dataset.INDEX_ZZ500, "zz500"),
    MacroHandler(Dataset.MACRO_DEPOSIT_RATE, "deposit_rate"),
    MacroHandler(Dataset.MACRO_LOAN_RATE, "loan_rate"),
    MacroHandler(Dataset.MACRO_RRR, "rrr"),
    MacroHandler(Dataset.MACRO_MONEY_SUPPLY_MONTH, "money_supply_month"),
    MacroHandler(Dataset.MACRO_MONEY_SUPPLY_YEAR, "money_supply_year"),
]

# 每码日频 pass（顺序移植自旧 dotnet SyncStockAsync）
CODE_HANDLERS = [
    StockBasicHandler(),
    KlineHandler(Dataset.K_D, "d"),
    KlineHandler(Dataset.K_W, "w"),
    AdjustFactorHandler(),
    DividendHandler(),
    FinancialHandler(),
    ReportEventHandler(
        Dataset.PERFORMANCE_EXPRESS, "query_performance_express",
        "performanceExpStatDate", "performanceExpPubDate",
    ),
    ReportEventHandler(
        Dataset.FORECAST, "query_forecast",
        "profitForcastExpStatDate", "profitForcastExpPubDate",
    ),
]

# 分钟线独立第二遍（全部码的日频 pass 结束后再跑，让日线先可用）
MINUTE_HANDLERS = [
    KlineMinuteHandler(Dataset.K_5, "5"),
    KlineMinuteHandler(Dataset.K_30, "30"),
]
