"""
DataFrame → PostgreSQL 批量 upsert。

baostock 全部返回字符串，本模块负责类型解析（→ Decimal/date/bool/int）、
列名映射（camelCase → snake_case，显式映射表），以及水位表更新。

**落库与水位更新必须在同一事务**（由调用方的 session 保证）：
任务被 SIGKILL 时要么都提交、要么都回滚，不会出现"有数据无水位"或反之。
"""

import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from core.timeutil import CST as _CST

from db.models import (
    AdjustFactor,
    DataWatermark,
    DepositRate,
    Dividend,
    FinancialReport,
    IndexConstituent,
    Kline,
    KlineMinute,
    LoanRate,
    MoneySupplyMonth,
    MoneySupplyYear,
    RequiredReserveRatio,
    StockBasic,
    StockIndustry,
    StockListSnapshot,
    TradeCalendar,
)

logger = logging.getLogger(__name__)

_CHUNK = 1000


# ── 类型解析（空串/异常 → None） ──


def _dec(s) -> Decimal | None:
    if s is None or s == "":
        return None
    try:
        return Decimal(str(s))
    except InvalidOperation:
        return None


def _int(s) -> int | None:
    if s is None or s == "":
        return None
    try:
        return int(str(s))
    except ValueError:
        return None


def _date(s) -> date | None:
    if s is None or s == "":
        return None
    try:
        return datetime.strptime(str(s), "%Y-%m-%d").date()
    except ValueError:
        return None


def _bool01(s) -> bool | None:
    if s is None or s == "":
        return None
    return str(s) == "1"


def _bar_time(s) -> datetime:
    """分钟线 time 字段：YYYYMMDDHHMMSSsss → 带 +08 时区的 datetime。"""
    return datetime.strptime(str(s)[:14], "%Y%m%d%H%M%S").replace(tzinfo=_CST)


# ── 通用 upsert ──


def upsert_rows(session: Session, model, rows: list[dict]) -> int:
    """按主键 ON CONFLICT DO UPDATE 批量写入，updated_at 强制刷新。"""
    if not rows:
        return 0
    table = model.__table__
    pk = [c.name for c in table.primary_key]
    total = 0
    for i in range(0, len(rows), _CHUNK):
        chunk = rows[i : i + _CHUNK]
        stmt = pg_insert(table).values(chunk)
        update_cols = {
            c.name: stmt.excluded[c.name]
            for c in table.columns
            if c.name not in pk and c.name != "updated_at"
        }
        update_cols["updated_at"] = func.now()
        stmt = stmt.on_conflict_do_update(index_elements=pk, set_=update_cols)
        session.execute(stmt)
        total += len(chunk)
    return total


def update_watermark(
    session: Session,
    data_type: str,
    last_date: date,
    first_date: date | None = None,
    code: str = "",
) -> None:
    """水位 upsert：first_date 取更早、last_date 取更晚（PG GREATEST/LEAST 忽略 NULL）。"""
    stmt = pg_insert(DataWatermark.__table__).values(
        code=code,
        data_type=data_type,
        first_date=first_date,
        last_date=last_date,
        last_fetched_at=func.now(),
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["code", "data_type"],
        set_={
            "first_date": func.least(
                DataWatermark.__table__.c.first_date, stmt.excluded.first_date
            ),
            "last_date": func.greatest(
                DataWatermark.__table__.c.last_date, stmt.excluded.last_date
            ),
            "last_fetched_at": func.now(),
        },
    )
    session.execute(stmt)


# ── 各数据集的行构建 ──

# kline 列映射：baostock 字段 → (模型列, 转换器)
_K_COL_MAP = {
    "date": ("trade_date", _date),
    "open": ("open", _dec),
    "high": ("high", _dec),
    "low": ("low", _dec),
    "close": ("close", _dec),
    "preclose": ("preclose", _dec),
    "volume": ("volume", _int),
    "amount": ("amount", _dec),
    "turn": ("turn", _dec),
    "pctChg": ("pct_chg", _dec),
    "tradestatus": ("trade_status", _int),
    "isST": ("is_st", _bool01),
    "peTTM": ("pe_ttm", _dec),
    "pbMRQ": ("pb_mrq", _dec),
    "psTTM": ("ps_ttm", _dec),
    "pcfNcfTTM": ("pcf_ncf_ttm", _dec),
}


def write_kline(session: Session, df: pd.DataFrame, code: str, frequency: str) -> int:
    rows = []
    for rec in df.to_dict("records"):
        row: dict = {"code": code, "frequency": frequency}
        for src, (dst, conv) in _K_COL_MAP.items():
            if src in rec:
                row[dst] = conv(rec[src])
        rows.append(row)
    return upsert_rows(session, Kline, rows)


def write_kline_minute(session: Session, df: pd.DataFrame, code: str, frequency: int) -> int:
    rows = [
        {
            "code": code,
            "frequency": frequency,
            "bar_time": _bar_time(rec["time"]),
            "open": _dec(rec.get("open")),
            "high": _dec(rec.get("high")),
            "low": _dec(rec.get("low")),
            "close": _dec(rec.get("close")),
            "volume": _int(rec.get("volume")),
            "amount": _dec(rec.get("amount")),
        }
        for rec in df.to_dict("records")
    ]
    return upsert_rows(session, KlineMinute, rows)


def write_adjust_factor(session: Session, df: pd.DataFrame, code: str) -> int:
    rows = [
        {
            "code": code,
            "divid_operate_date": _date(rec["dividOperateDate"]),
            "fore_adjust_factor": _dec(rec["foreAdjustFactor"]),
            "back_adjust_factor": _dec(rec["backAdjustFactor"]),
            "adjust_factor": _dec(rec.get("adjustFactor")),
        }
        for rec in df.to_dict("records")
        if _date(rec.get("dividOperateDate"))
    ]
    return upsert_rows(session, AdjustFactor, rows)


# dividend：关键字段落列，其余进 detail
_DIVIDEND_TYPED = {
    "dividRegistDate": ("regist_date", _date),
    "dividOperateDate": ("operate_date", _date),
    "dividPayDate": ("pay_date", _date),
    "dividCashPsBeforeTax": ("cash_ps_before_tax", _dec),
    "dividCashPsAfterTax": ("cash_ps_after_tax", _dec),
    "dividStocksPs": ("stocks_ps", _dec),
    "dividReserveToStockPs": ("reserve_to_stock_ps", _dec),
}


def write_dividend(
    session: Session, df: pd.DataFrame, code: str, year: int, year_type: str
) -> int:
    rows = []
    for rec in df.to_dict("records"):
        plan_date = _date(rec.get("dividPlanAnnounceDate"))
        if plan_date is None:  # 预案公告日缺失无法定位主键，跳过
            logger.warning("分红记录缺少预案公告日，跳过: %s %s", code, rec)
            continue
        row: dict = {
            "code": code,
            "plan_announce_date": plan_date,
            "year_type": year_type,
            "year": year,
        }
        detail = {}
        for src, val in rec.items():
            if src in ("code", "dividPlanAnnounceDate"):
                continue
            if src in _DIVIDEND_TYPED:
                dst, conv = _DIVIDEND_TYPED[src]
                row[dst] = conv(val)
            else:
                detail[src] = val
        row["detail"] = detail or None
        rows.append(row)
    return upsert_rows(session, Dividend, rows)


def write_financial_reports(
    session: Session, code: str, report_type: str, records: list[dict],
    stat_key: str = "statDate", pub_key: str = "pubDate",
) -> int:
    """财报 upsert：stat_date/pub_date 提为列，其余字段全部进 metrics（JSONB）。"""
    rows = []
    for rec in records:
        stat_date = _date(rec.get(stat_key))
        if stat_date is None:
            logger.warning("财报记录缺少报告期，跳过: %s %s %s", code, report_type, rec)
            continue
        metrics = {k: v for k, v in rec.items() if k not in ("code", stat_key, pub_key)}
        rows.append(
            {
                "code": code,
                "report_type": report_type,
                "stat_date": stat_date,
                "pub_date": _date(rec.get(pub_key)),
                "metrics": metrics,
            }
        )
    return upsert_rows(session, FinancialReport, rows)


def write_stock_basic(session: Session, df: pd.DataFrame) -> int:
    rows = [
        {
            "code": rec["code"],
            "code_name": rec.get("code_name") or None,
            "ipo_date": _date(rec.get("ipoDate")),
            "out_date": _date(rec.get("outDate")),
            "type": _int(rec.get("type")),
            "status": _int(rec.get("status")),
        }
        for rec in df.to_dict("records")
    ]
    return upsert_rows(session, StockBasic, rows)


def write_trade_calendar(session: Session, df: pd.DataFrame) -> int:
    rows = [
        {
            "calendar_date": _date(rec["calendar_date"]),
            "is_trading_day": _bool01(rec["is_trading_day"]),
        }
        for rec in df.to_dict("records")
    ]
    return upsert_rows(session, TradeCalendar, rows)


def write_stock_list(session: Session, df: pd.DataFrame, snap_date: date) -> int:
    rows = [
        {
            "snap_date": snap_date,
            "code": rec["code"],
            "code_name": rec.get("code_name") or None,
            "trade_status": _bool01(rec.get("tradeStatus")),
        }
        for rec in df.to_dict("records")
    ]
    return upsert_rows(session, StockListSnapshot, rows)


def write_index_constituent(
    session: Session, df: pd.DataFrame, index_code: str, snap_date: date
) -> int:
    rows = [
        {
            "index_code": index_code,
            "snap_date": snap_date,
            "code": rec["code"],
            "code_name": rec.get("code_name") or None,
        }
        for rec in df.to_dict("records")
    ]
    return upsert_rows(session, IndexConstituent, rows)


def write_industry(session: Session, df: pd.DataFrame, snap_date: date) -> int:
    rows = [
        {
            "snap_date": snap_date,
            "code": rec["code"],
            "code_name": rec.get("code_name") or None,
            "industry": rec.get("industry") or None,
            "industry_classification": rec.get("industryClassification") or None,
        }
        for rec in df.to_dict("records")
    ]
    return upsert_rows(session, StockIndustry, rows)


# ── 宏观：每类一个显式列映射（含 baostock 的 mortgate 拼写修正） ──

_MACRO_SPECS: dict[str, tuple] = {
    "deposit_rate": (
        DepositRate,
        {
            "pubDate": ("pub_date", _date),
            "demandDepositRate": ("demand_deposit_rate", _dec),
            "fixedDepositRate3Month": ("fixed_deposit_rate_3month", _dec),
            "fixedDepositRate6Month": ("fixed_deposit_rate_6month", _dec),
            "fixedDepositRate1Year": ("fixed_deposit_rate_1year", _dec),
            "fixedDepositRate2Year": ("fixed_deposit_rate_2year", _dec),
            "fixedDepositRate3Year": ("fixed_deposit_rate_3year", _dec),
            "fixedDepositRate5Year": ("fixed_deposit_rate_5year", _dec),
            "installmentFixedDepositRate1Year": ("installment_fixed_deposit_rate_1year", _dec),
            "installmentFixedDepositRate3Year": ("installment_fixed_deposit_rate_3year", _dec),
            "installmentFixedDepositRate5Year": ("installment_fixed_deposit_rate_5year", _dec),
        },
    ),
    "loan_rate": (
        LoanRate,
        {
            "pubDate": ("pub_date", _date),
            "loanRate6Month": ("loan_rate_6month", _dec),
            "loanRate6MonthTo1Year": ("loan_rate_6month_to_1year", _dec),
            "loanRate1YearTo3Year": ("loan_rate_1year_to_3year", _dec),
            "loanRate3YearTo5Year": ("loan_rate_3year_to_5year", _dec),
            "loanRateAbove5Year": ("loan_rate_above_5year", _dec),
            "mortgateRateBelow5Year": ("mortgage_rate_below_5year", _dec),
            "mortgateRateAbove5Year": ("mortgage_rate_above_5year", _dec),
        },
    ),
    "rrr": (
        RequiredReserveRatio,
        {
            "pubDate": ("pub_date", _date),
            "effectiveDate": ("effective_date", _date),
            "bigInstitutionsRatioPre": ("big_institutions_ratio_pre", _dec),
            "bigInstitutionsRatioAfter": ("big_institutions_ratio_after", _dec),
            "mediumInstitutionsRatioPre": ("medium_institutions_ratio_pre", _dec),
            "mediumInstitutionsRatioAfter": ("medium_institutions_ratio_after", _dec),
        },
    ),
    "money_supply_month": (
        MoneySupplyMonth,
        {
            "statYear": ("stat_year", _int),
            "statMonth": ("stat_month", _int),
            "m0Month": ("m0_month", _dec),
            "m0YOY": ("m0_yoy", _dec),
            "m0ChainRelative": ("m0_chain_relative", _dec),
            "m1Month": ("m1_month", _dec),
            "m1YOY": ("m1_yoy", _dec),
            "m1ChainRelative": ("m1_chain_relative", _dec),
            "m2Month": ("m2_month", _dec),
            "m2YOY": ("m2_yoy", _dec),
            "m2ChainRelative": ("m2_chain_relative", _dec),
        },
    ),
    "money_supply_year": (
        MoneySupplyYear,
        {
            "statYear": ("stat_year", _int),
            "m0Year": ("m0_year", _dec),
            "m0YearYOY": ("m0_year_yoy", _dec),
            "m1Year": ("m1_year", _dec),
            "m1YearYOY": ("m1_year_yoy", _dec),
            "m2Year": ("m2_year", _dec),
            "m2YearYOY": ("m2_year_yoy", _dec),
        },
    ),
}


def write_macro(session: Session, df: pd.DataFrame, kind: str) -> int:
    model, col_map = _MACRO_SPECS[kind]
    rows = []
    for rec in df.to_dict("records"):
        row = {}
        for src, (dst, conv) in col_map.items():
            if src in rec:
                row[dst] = conv(rec[src])
        rows.append(row)
    return upsert_rows(session, model, rows)
