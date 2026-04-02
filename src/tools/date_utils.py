"""
交易日期工具模块。

提供与 A 股交易日期相关的 MCP 工具，包括：
- 获取最新交易日
- 获取市场分析时间范围
- 判断指定日期是否为交易日
- 查询前一个/后一个交易日
- 获取最近 N 个交易日列表
- 获取最近 N 个交易日的日期范围
- 获取指定年份各月末交易日

这些工具是日期敏感型分析的基础，LLM 应优先调用这些工具确定日期，
而非依赖自身的训练数据推断当前日期。
"""
import logging

from src.data_source import active_data_source
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.core import date_utils as uc_date

logger = logging.getLogger(__name__)


@app.tool()
def get_latest_trading_date() -> str:
    """Get the latest trading date up to today."""
    logger.info("Tool 'get_latest_trading_date' called")
    return run_tool_with_handling(
        lambda: uc_date.get_latest_trading_date(active_data_source),
        context="get_latest_trading_date",
    )


@app.tool()
def get_market_analysis_timeframe(period: str = "recent") -> str:
    """Return a human-friendly timeframe label."""
    logger.info(f"Tool 'get_market_analysis_timeframe' called with period={period}")
    return run_tool_with_handling(
        lambda: uc_date.get_market_analysis_timeframe(period=period),
        context="get_market_analysis_timeframe",
    )


@app.tool()
def is_trading_day(date: str) -> str:
    """Check if a specific date is a trading day."""
    return run_tool_with_handling(
        lambda: uc_date.is_trading_day(active_data_source, date=date),
        context=f"is_trading_day:{date}",
    )


@app.tool()
def previous_trading_day(date: str) -> str:
    """Get the previous trading day before the given date."""
    return run_tool_with_handling(
        lambda: uc_date.previous_trading_day(active_data_source, date=date),
        context=f"previous_trading_day:{date}",
    )


@app.tool()
def next_trading_day(date: str) -> str:
    """Get the next trading day after the given date."""
    return run_tool_with_handling(
        lambda: uc_date.next_trading_day(active_data_source, date=date),
        context=f"next_trading_day:{date}",
    )


@app.tool()
def get_last_n_trading_days(days: int = 5) -> str:
    """Return the last N trading dates."""
    return run_tool_with_handling(
        lambda: uc_date.get_last_n_trading_days(active_data_source, days=days),
        context=f"get_last_n_trading_days:{days}",
    )


@app.tool()
def get_recent_trading_range(days: int = 5) -> str:
    """Return a date range string covering the recent N trading days."""
    return run_tool_with_handling(
        lambda: uc_date.get_recent_trading_range(active_data_source, days=days),
        context=f"get_recent_trading_range:{days}",
    )


@app.tool()
def get_month_end_trading_dates(year: int) -> str:
    """Return month-end trading dates for a given year."""
    return run_tool_with_handling(
        lambda: uc_date.get_month_end_trading_dates(active_data_source, year=year),
        context=f"get_month_end_trading_dates:{year}",
    )
