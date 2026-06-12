"""股票行情：K 线、基本信息、分红、复权因子、分析报告。"""

from datetime import date
from typing import Literal

from fastapi import APIRouter, Query
from fastapi.responses import PlainTextResponse

from api.services import analysis, kline, market
from core.helpers import normalize_stock_code_logic
from fetcher.providers.interface import NoDataFoundError

router = APIRouter(prefix="/api/v1/stocks", tags=["stocks"])


@router.get("/{code}/kline")
def get_kline(
    code: str,
    start_date: date,
    end_date: date,
    frequency: Literal["d", "w", "m"] = "d",
    adjust_flag: Literal["1", "2", "3"] = "3",
):
    code = normalize_stock_code_logic(code)
    return kline.get_kline(code, start_date, end_date, frequency, adjust_flag)


@router.get("/{code}/kline-minute")
def get_kline_minute(
    code: str,
    start_date: date,
    end_date: date,
    frequency: Literal["5", "15", "30", "60"] = "30",
):
    code = normalize_stock_code_logic(code)
    return kline.get_kline_minute(code, start_date, end_date, int(frequency))


@router.get("/{code}/basic")
def get_basic(code: str):
    code = normalize_stock_code_logic(code)
    info = market.get_stock_basic(code)
    if info is None:
        raise NoDataFoundError(f"未找到股票 {code} 的基本信息")
    return info


@router.get("/{code}/dividends")
def get_dividends(
    code: str,
    year: int = Query(ge=1990, le=2100),
    year_type: Literal["report", "operate"] = "report",
):
    code = normalize_stock_code_logic(code)
    return market.get_dividends(code, year, year_type)


@router.get("/{code}/adjust-factors")
def get_adjust_factors(code: str, start_date: date, end_date: date):
    code = normalize_stock_code_logic(code)
    return kline.get_adjust_factors(code, start_date, end_date)


@router.get("/{code}/analysis", response_class=PlainTextResponse)
def get_analysis(
    code: str,
    analysis_type: Literal["fundamental", "technical", "comprehensive"] = "comprehensive",
):
    code = normalize_stock_code_logic(code)
    return analysis.build_stock_analysis_report(code, analysis_type)
