"""市场概览：交易日历、全部股票列表。"""

from datetime import date

from fastapi import APIRouter

from api.services import market
from api.services.dates import latest_trading_date

router = APIRouter(prefix="/api/v1/market", tags=["market"])


@router.get("/trade-calendar")
def get_trade_calendar(start_date: date, end_date: date):
    return market.get_trade_calendar(start_date, end_date)


@router.get("/stocks")
def get_stock_list(snap_date: date | None = None):
    if snap_date is not None:
        return market.get_stock_list(snap_date)
    return market.get_stock_list(latest_trading_date(), allow_fallback=True)
