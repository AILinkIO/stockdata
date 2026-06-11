"""指数成分股与行业分类。"""

from datetime import date
from typing import Literal

from fastapi import APIRouter

from api.services import market
from api.services.dates import latest_trading_date
from core.helpers import normalize_stock_code_logic

router = APIRouter(prefix="/api/v1", tags=["indices"])


@router.get("/indices/{index_code}/constituents")
def get_constituents(index_code: Literal["sz50", "hs300", "zz500"],
                     snap_date: date | None = None):
    return market.get_index_constituents(index_code, snap_date or latest_trading_date())


@router.get("/industries")
def get_industries(snap_date: date | None = None, code: str | None = None):
    if code:
        code = normalize_stock_code_logic(code)
    return market.get_industry(snap_date or latest_trading_date(), code=code)
