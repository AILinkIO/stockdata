"""
股票行情数据工具模块。

提供 A 股个股行情相关的 MCP 工具，包括：
- 历史 K 线数据（日/周/月/分钟级别）
- 股票基本信息查询
- 分红送转数据
- 复权因子数据
"""
import logging
from typing import List, Optional

from src.data_source import active_data_source
from src.formatting.markdown_formatter import format_table_output
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.services.validation import (
    validate_adjust_flag,
    validate_frequency,
    validate_output_format,
    validate_year,
    validate_year_type,
)

logger = logging.getLogger(__name__)


@app.tool()
def get_historical_k_data(
    code: str,
    start_date: str,
    end_date: str,
    frequency: str = "d",
    adjust_flag: str = "3",
    fields: Optional[List[str]] = None,
    limit: int = 250,
    format: str = "markdown",
) -> str:
    """
    Fetches historical K-line (OHLCV) data for a Chinese A-share stock.

    Args:
        code: The stock code in Baostock format (e.g., 'sh.600000', 'sz.000001').
        start_date: Start date in 'YYYY-MM-DD' format.
        end_date: End date in 'YYYY-MM-DD' format.
        frequency: Data frequency. Valid options (from Baostock):
                     'd': daily
                     'w': weekly
                     'm': monthly
                     '5': 5 minutes
                     '15': 15 minutes
                     '30': 30 minutes
                     '60': 60 minutes
                   Defaults to 'd'.
        adjust_flag: Adjustment flag for price/volume. Valid options (from Baostock):
                       '1': Forward adjusted (后复权)
                       '2': Backward adjusted (前复权)
                       '3': Non-adjusted (不复权)
                     Defaults to '3'.
        fields: Optional list of specific data fields to retrieve (must be valid Baostock fields).
                If None or empty, default fields will be used (e.g., date, code, open, high, low, close, volume, amount, pctChg).
        limit: Max rows to return. Defaults to 250.
        format: Output format: 'markdown' | 'json' | 'csv'. Defaults to 'markdown'.

        Returns:
            A Markdown formatted string containing the K-line data table, or an error message.
            The table might be truncated if the result set is too large.
        """
    logger.info(
        f"Tool 'get_historical_k_data' called for {code} ({start_date}-{end_date}, freq={frequency}, adj={adjust_flag}, fields={fields})"
    )

    def action():
        validate_frequency(frequency)
        validate_adjust_flag(adjust_flag)
        validate_output_format(format)
        df = active_data_source.get_historical_k_data(
            code=code, start_date=start_date, end_date=end_date,
            frequency=frequency, adjust_flag=adjust_flag, fields=fields,
        )
        meta = {"code": code, "start_date": start_date, "end_date": end_date,
                "frequency": frequency, "adjust_flag": adjust_flag}
        return format_table_output(df, format=format, max_rows=limit, meta=meta)

    return run_tool_with_handling(action, context=f"get_historical_k_data:{code}")


@app.tool()
def get_stock_basic_info(code: str, fields: Optional[List[str]] = None, format: str = "markdown") -> str:
    """
    Fetches basic information for a given Chinese A-share stock.

    Args:
        code: The stock code in Baostock format (e.g., 'sh.600000', 'sz.000001').
        fields: Optional list to select specific columns from the available basic info
                (e.g., ['code', 'code_name', 'industry', 'listingDate']).
                If None or empty, returns all available basic info columns from Baostock.

    Returns:
        Basic stock information in the requested format.
    """
    logger.info(f"Tool 'get_stock_basic_info' called for {code} (fields={fields})")

    def action():
        validate_output_format(format)
        df = active_data_source.get_stock_basic_info(code=code, fields=fields)
        meta = {"code": code}
        return format_table_output(df, format=format, max_rows=df.shape[0] if df is not None else 0, meta=meta)

    return run_tool_with_handling(action, context=f"get_stock_basic_info:{code}")


@app.tool()
def get_dividend_data(code: str, year: str, year_type: str = "report", limit: int = 250, format: str = "markdown") -> str:
    """
    Fetches dividend information for a given stock code and year.

    Args:
        code: The stock code in Baostock format (e.g., 'sh.600000', 'sz.000001').
        year: The year to query (e.g., '2023').
        year_type: Type of year. Valid options (from Baostock):
                     'report': Announcement year (预案公告年份)
                     'operate': Ex-dividend year (除权除息年份)
                   Defaults to 'report'.

    Returns:
        Dividend records table.
    """
    logger.info(f"Tool 'get_dividend_data' called for {code}, year={year}, year_type={year_type}")

    def action():
        validate_year(year)
        validate_year_type(year_type)
        validate_output_format(format)
        df = active_data_source.get_dividend_data(code=code, year=year, year_type=year_type)
        meta = {"code": code, "year": year, "year_type": year_type}
        return format_table_output(df, format=format, max_rows=limit, meta=meta)

    return run_tool_with_handling(action, context=f"get_dividend_data:{code}:{year}")


@app.tool()
def get_adjust_factor_data(code: str, start_date: str, end_date: str, limit: int = 250, format: str = "markdown") -> str:
    """
    Fetches adjustment factor data for a given stock code and date range.
    Uses Baostock's "涨跌幅复权算法" factors. Useful for calculating adjusted prices.

    Args:
        code: The stock code in Baostock format (e.g., 'sh.600000', 'sz.000001').
        start_date: Start date in 'YYYY-MM-DD' format.
        end_date: End date in 'YYYY-MM-DD' format.

    Returns:
        Adjustment factors table.
    """
    logger.info(f"Tool 'get_adjust_factor_data' called for {code} ({start_date} to {end_date})")

    def action():
        validate_output_format(format)
        df = active_data_source.get_adjust_factor_data(code=code, start_date=start_date, end_date=end_date)
        meta = {"code": code, "start_date": start_date, "end_date": end_date}
        return format_table_output(df, format=format, max_rows=limit, meta=meta)

    return run_tool_with_handling(action, context=f"get_adjust_factor_data:{code}")
