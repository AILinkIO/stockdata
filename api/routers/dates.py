"""交易日工具。"""

from datetime import date

from fastapi import APIRouter, Query

from api.services import dates

router = APIRouter(prefix="/api/v1/dates", tags=["dates"])


@router.get("/latest-trading-day")
def latest_trading_day():
    return {"date": dates.latest_trading_date()}


@router.get("/is-trading-day")
def is_trading_day(date_: date = Query(alias="date")):
    return {"date": date_, "is_trading_day": dates.is_trading_day(date_)}


@router.get("/previous-trading-day")
def previous_trading_day(date_: date = Query(alias="date")):
    return {"date": dates.previous_trading_day(date_)}


@router.get("/next-trading-day")
def next_trading_day(date_: date = Query(alias="date")):
    return {"date": dates.next_trading_day(date_)}


@router.get("/last-trading-days")
def last_trading_days(days: int = Query(default=10, ge=1, le=250)):
    return {"dates": dates.last_n_trading_days(days)}
