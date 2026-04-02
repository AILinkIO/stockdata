"""
市场概览工具模块。

提供 A 股市场整体概况相关的 MCP 工具，包括：
- 交易日历查询（指定日期范围内的交易日/非交易日）
- 全部股票列表及交易状态
- 股票代码模糊搜索
- 停牌股票查询
"""
import logging
from typing import Optional

from src.data_source import active_data_source
from src.formatting.markdown_formatter import format_table_output
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.services.validation import validate_non_empty_str, validate_output_format

logger = logging.getLogger(__name__)


@app.tool()
def get_trade_dates(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """
    Fetch trading dates within a specified range.

    Args:
        start_date: Optional. Start date in 'YYYY-MM-DD' format. Defaults to 2015-01-01 if None.
        end_date: Optional. End date in 'YYYY-MM-DD' format. Defaults to the current date if None.

    Returns:
        Markdown table with 'is_trading_day' (1=trading, 0=non-trading).
    """
    logger.info(f"Tool 'get_trade_dates' called for range {start_date or 'default'} to {end_date or 'default'}")

    def action():
        validate_output_format(format)
        df = active_data_source.get_trade_dates(start_date=start_date, end_date=end_date)
        meta = {"start_date": start_date or "default", "end_date": end_date or "default"}
        return format_table_output(df, format=format, max_rows=limit, meta=meta)

    return run_tool_with_handling(action, context="get_trade_dates")


@app.tool()
def get_all_stock(date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """
    Fetch a list of all stocks (A-shares and indices) and their trading status for a date.

    Args:
        date: Optional. The date in 'YYYY-MM-DD' format. If None, uses the current date.

    Returns:
        Markdown table listing stock codes and trading status (1=trading, 0=suspended).
    """
    logger.info(f"Tool 'get_all_stock' called for date={date or 'default'}")

    def action():
        validate_output_format(format)
        df = active_data_source.get_all_stock(date=date)
        meta = {"as_of": date or "default"}
        return format_table_output(df, format=format, max_rows=limit, meta=meta)

    return run_tool_with_handling(action, context=f"get_all_stock:{date or 'default'}")


@app.tool()
def search_stocks(keyword: str, date: Optional[str] = None, limit: int = 50, format: str = "markdown") -> str:
    """
    Search stocks by code substring on a date.

    Args:
        keyword: Substring to match in the stock code (e.g., '600', '000001').
        date: Optional 'YYYY-MM-DD'. If None, uses current date.
        limit: Max rows to return. Defaults to 50.
        format: Output format: 'markdown' | 'json' | 'csv'. Defaults to 'markdown'.

    Returns:
        Matching stock codes with their trading status.
    """
    logger.info("Tool 'search_stocks' called keyword=%s, date=%s, limit=%s, format=%s", keyword, date or "default", limit, format)

    def action():
        validate_output_format(format)
        validate_non_empty_str(keyword, "keyword")
        df = active_data_source.get_all_stock(date=date)
        if df is None or df.empty:
            return "(No data available to display)"
        filtered = df[df["code"].str.lower().str.contains(keyword.strip().lower(), na=False)]
        meta = {"keyword": keyword, "as_of": date or "current"}
        return format_table_output(filtered, format=format, max_rows=limit, meta=meta)

    return run_tool_with_handling(action, context=f"search_stocks:{keyword}")


@app.tool()
def get_suspensions(date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """
    List suspended stocks for a date.

    Args:
        date: Optional 'YYYY-MM-DD'. If None, uses current date.
        limit: Max rows to return. Defaults to 250.
        format: Output format: 'markdown' | 'json' | 'csv'. Defaults to 'markdown'.

    Returns:
        Table of stocks where tradeStatus==0.
    """
    logger.info("Tool 'get_suspensions' called date=%s, limit=%s, format=%s", date or "current", limit, format)

    def action():
        validate_output_format(format)
        df = active_data_source.get_all_stock(date=date)
        if df is None or df.empty:
            return "(No data available to display)"
        if "tradeStatus" not in df.columns:
            raise ValueError("'tradeStatus' column not present in data source response.")
        suspended = df[df["tradeStatus"] == '0']
        meta = {"as_of": date or "current", "total_suspended": int(suspended.shape[0])}
        return format_table_output(suspended, format=format, max_rows=limit, meta=meta)

    return run_tool_with_handling(action, context=f"get_suspensions:{date or 'current'}")
