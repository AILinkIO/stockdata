"""宏观经济：存贷款利率、存款准备金率、货币供应量。"""

from datetime import date, timedelta

from fastapi import APIRouter

from api.services import market
from api.services.readthrough import today

router = APIRouter(prefix="/api/v1/macro", tags=["macro"])

_DEFAULT_SPAN_DAYS = 3650  # 默认回看 10 年


def _default_range(start_date: date | None, end_date: date | None) -> tuple[date, date]:
    end = end_date or today()
    start = start_date or end - timedelta(days=_DEFAULT_SPAN_DAYS)
    return start, end


@router.get("/deposit-rate")
def deposit_rate(start_date: date | None = None, end_date: date | None = None):
    return market.get_macro_rates("deposit_rate", *_default_range(start_date, end_date))


@router.get("/loan-rate")
def loan_rate(start_date: date | None = None, end_date: date | None = None):
    return market.get_macro_rates("loan_rate", *_default_range(start_date, end_date))


@router.get("/rrr")
def required_reserve_ratio(start_date: date | None = None, end_date: date | None = None):
    return market.get_macro_rates("rrr", *_default_range(start_date, end_date))


@router.get("/money-supply/month")
def money_supply_month(start_date: date | None = None, end_date: date | None = None):
    return market.get_money_supply_month(*_default_range(start_date, end_date))


@router.get("/money-supply/year")
def money_supply_year(start_year: int | None = None, end_year: int | None = None):
    end = end_year or today().year
    start = start_year or end - 10
    return market.get_money_supply_year(start, end)
