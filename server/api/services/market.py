"""市场概览/指数/行业/宏观/基本信息/分红 的读取服务。"""

from datetime import date

from sqlalchemy import exists, select, tuple_

from api.services.readthrough import (
    ensure_dividend,
    ensure_range,
    ensure_snapshot,
    ensure_stock_basic,
)
from db.models import (
    DepositRate,
    Dividend,
    IndexConstituent,
    LoanRate,
    MoneySupplyMonth,
    MoneySupplyYear,
    RequiredReserveRatio,
    StockBasic,
    StockIndustry,
    StockListSnapshot,
    TradeCalendar,
    model_columns,
)
from db.session import SyncSession


def _rows(stmt, cols) -> list[dict]:
    with SyncSession() as s:
        return [
            {c: getattr(r, c) for c in cols} for r in s.scalars(stmt).all()
        ]


def get_trade_calendar(start: date, end: date) -> list[dict]:
    ensure_range("trade_calendar", start, end)
    return _rows(
        select(TradeCalendar)
        .where(TradeCalendar.calendar_date >= start, TradeCalendar.calendar_date <= end)
        .order_by(TradeCalendar.calendar_date),
        ["calendar_date", "is_trading_day"],
    )


def get_stock_basic(code: str) -> dict | None:
    with SyncSession() as s:
        has = s.execute(select(exists().where(StockBasic.code == code))).scalar()
    ensure_stock_basic(code, bool(has))
    with SyncSession() as s:
        r = s.get(StockBasic, code)
    if r is None:
        return None
    return {c: getattr(r, c) for c in
            ["code", "code_name", "ipo_date", "out_date", "type", "status"]}


def get_stock_list(snap_date: date, allow_fallback: bool = False) -> list[dict]:
    """全部股票列表快照。

    allow_fallback：当日列表盘中尚未发布时（抓取返回空），自动回退最近的
    前一交易日，最多回退 3 个交易日。仅在调用方未显式指定日期时启用。
    """
    from api.services.dates import previous_trading_day

    attempts = 4 if allow_fallback else 1
    for _ in range(attempts):
        with SyncSession() as s:
            has = s.execute(
                select(exists().where(StockListSnapshot.snap_date == snap_date))
            ).scalar()
        ensure_snapshot("stock_list", snap_date, bool(has))
        rows = _rows(
            select(StockListSnapshot)
            .where(StockListSnapshot.snap_date == snap_date)
            .order_by(StockListSnapshot.code),
            ["snap_date", "code", "code_name", "trade_status"],
        )
        if rows or not allow_fallback:
            return rows
        snap_date = previous_trading_day(snap_date)
    return []


def get_index_constituents(index_code: str, snap_date: date) -> list[dict]:
    with SyncSession() as s:
        has = s.execute(
            select(exists().where(IndexConstituent.index_code == index_code,
                                  IndexConstituent.snap_date == snap_date))
        ).scalar()
    ensure_snapshot(f"index_{index_code}", snap_date, bool(has))
    return _rows(
        select(IndexConstituent)
        .where(IndexConstituent.index_code == index_code,
               IndexConstituent.snap_date == snap_date)
        .order_by(IndexConstituent.code),
        ["index_code", "snap_date", "code", "code_name"],
    )


def get_industry(snap_date: date, code: str | None = None) -> list[dict]:
    with SyncSession() as s:
        has = s.execute(
            select(exists().where(StockIndustry.snap_date == snap_date))
        ).scalar()
    ensure_snapshot("industry", snap_date, bool(has))
    stmt = select(StockIndustry).where(StockIndustry.snap_date == snap_date)
    if code:
        stmt = stmt.where(StockIndustry.code == code)
    return _rows(
        stmt.order_by(StockIndustry.code),
        ["snap_date", "code", "code_name", "industry", "industry_classification"],
    )


def get_dividends(code: str, year: int, year_type: str = "report") -> list[dict]:
    ensure_dividend(code, year, year_type)
    return _rows(
        select(Dividend)
        .where(Dividend.code == code, Dividend.year == year,
               Dividend.year_type == year_type)
        .order_by(Dividend.plan_announce_date),
        ["code", "year", "year_type", "plan_announce_date", "regist_date",
         "operate_date", "pay_date", "cash_ps_before_tax", "cash_ps_after_tax",
         "stocks_ps", "reserve_to_stock_ps", "detail"],
    )


# ── 宏观 ──

_MACRO_READERS = {
    "deposit_rate": (DepositRate, DepositRate.pub_date),
    "loan_rate": (LoanRate, LoanRate.pub_date),
    "rrr": (RequiredReserveRatio, RequiredReserveRatio.pub_date),
}


def get_macro_rates(kind: str, start: date, end: date) -> list[dict]:
    """存款利率 / 贷款利率 / 存款准备金率。"""
    ensure_range(kind, start, end)
    model, date_col = _MACRO_READERS[kind]
    cols = model_columns(model)
    return _rows(
        select(model).where(date_col >= start, date_col <= end).order_by(date_col),
        cols,
    )


def get_money_supply_month(start: date, end: date) -> list[dict]:
    ensure_range("money_supply_month", start, end)
    cols = model_columns(MoneySupplyMonth)
    ym = tuple_(MoneySupplyMonth.stat_year, MoneySupplyMonth.stat_month)
    return _rows(
        select(MoneySupplyMonth)
        .where(ym >= (start.year, start.month), ym <= (end.year, end.month))
        .order_by(MoneySupplyMonth.stat_year, MoneySupplyMonth.stat_month),
        cols,
    )


def get_money_supply_year(start_year: int, end_year: int) -> list[dict]:
    ensure_range("money_supply_year", date(start_year, 1, 1), date(end_year, 1, 1))
    cols = model_columns(MoneySupplyYear)
    return _rows(
        select(MoneySupplyYear)
        .where(MoneySupplyYear.stat_year >= start_year,
               MoneySupplyYear.stat_year <= end_year)
        .order_by(MoneySupplyYear.stat_year),
        cols,
    )
