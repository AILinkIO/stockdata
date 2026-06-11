"""财务报表：六类季度财报、综合指标、业绩快报/预告。"""

from datetime import date
from typing import Literal

from fastapi import APIRouter, Query

from api.services import financial
from core.helpers import normalize_stock_code_logic

router = APIRouter(prefix="/api/v1/stocks", tags=["financials"])

_QuarterlyType = Literal["profit", "operation", "growth", "balance", "cash_flow", "dupont"]


@router.get("/{code}/financials/indicator")
def get_indicator(code: str, start_date: date, end_date: date):
    code = normalize_stock_code_logic(code)
    return financial.get_indicator(code, start_date, end_date)


@router.get("/{code}/financials/express")
def get_express(code: str, start_date: date, end_date: date):
    code = normalize_stock_code_logic(code)
    return financial.get_performance(code, start_date, end_date, "express")


@router.get("/{code}/financials/forecast")
def get_forecast(code: str, start_date: date, end_date: date):
    code = normalize_stock_code_logic(code)
    return financial.get_performance(code, start_date, end_date, "forecast")


@router.get("/{code}/financials/{report_type}")
def get_quarterly(
    code: str,
    report_type: _QuarterlyType,
    year: int = Query(ge=1990, le=2100),
    quarter: int = Query(ge=1, le=4),
):
    code = normalize_stock_code_logic(code)
    return financial.get_quarterly(code, year, quarter, report_type)
