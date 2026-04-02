"""
交易日期工具模块。

提供与 A 股交易日期相关的 MCP 工具。
这些工具是日期敏感型分析的基础，LLM 应优先调用这些工具确定日期，
而非依赖自身的训练数据推断当前日期。
"""
from src.data_source import active_data_source
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.core import date_utils as core_date


@app.tool()
def get_latest_trading_date() -> str:
    """获取截至今天的最新交易日。"""
    return run_tool_with_handling(
        lambda: core_date.get_latest_trading_date(active_data_source),
        context="get_latest_trading_date",
    )


@app.tool()
def get_market_analysis_timeframe(period: str = "recent") -> str:
    """获取市场分析的时间范围。

    Args:
        period: 分析周期（'recent' 近期 / 'quarter' 本季度 / 'half_year' 半年 / 'year' 全年）
    """
    return run_tool_with_handling(
        lambda: core_date.get_market_analysis_timeframe(period=period),
        context="get_market_analysis_timeframe",
    )


@app.tool()
def is_trading_day(date: str) -> str:
    """判断指定日期是否为交易日，返回'是'或'否'。"""
    return run_tool_with_handling(
        lambda: core_date.is_trading_day(active_data_source, date=date),
        context=f"is_trading_day:{date}",
    )


@app.tool()
def previous_trading_day(date: str) -> str:
    """获取指定日期之前的最近一个交易日。"""
    return run_tool_with_handling(
        lambda: core_date.previous_trading_day(active_data_source, date=date),
        context=f"previous_trading_day:{date}",
    )


@app.tool()
def next_trading_day(date: str) -> str:
    """获取指定日期之后的最近一个交易日。"""
    return run_tool_with_handling(
        lambda: core_date.next_trading_day(active_data_source, date=date),
        context=f"next_trading_day:{date}",
    )


@app.tool()
def get_last_n_trading_days(days: int = 5) -> str:
    """获取最近 N 个交易日列表。"""
    return run_tool_with_handling(
        lambda: core_date.get_last_n_trading_days(active_data_source, days=days),
        context=f"get_last_n_trading_days:{days}",
    )


@app.tool()
def get_recent_trading_range(days: int = 5) -> str:
    """获取最近 N 个交易日的起止日期范围。"""
    return run_tool_with_handling(
        lambda: core_date.get_recent_trading_range(active_data_source, days=days),
        context=f"get_recent_trading_range:{days}",
    )


@app.tool()
def get_month_end_trading_dates(year: int) -> str:
    """获取指定年份每个月最后一个交易日。"""
    return run_tool_with_handling(
        lambda: core_date.get_month_end_trading_dates(active_data_source, year=year),
        context=f"get_month_end_trading_dates:{year}",
    )
