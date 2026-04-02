"""
市场概览工具模块。

提供 A 股市场整体概况相关的 MCP 工具，包括：
- 交易日历查询
- 全部股票列表及交易状态
- 股票代码模糊搜索
- 停牌股票查询
"""
from typing import Optional

from src.data_source import active_data_source
from src.formatting.markdown import format_table_output
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.services.validation import validate_non_empty_str, validate_output_format


@app.tool()
def get_trade_dates(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """获取指定日期范围内的交易日历。

    Args:
        start_date: 起始日期 'YYYY-MM-DD'，默认 2015-01-01
        end_date: 结束日期 'YYYY-MM-DD'，默认当天
        limit: 最大返回行数，默认 250
        format: 输出格式，默认 'markdown'

    Returns:
        包含 is_trading_day 列的表格（1=交易日, 0=非交易日）
    """
    def action():
        validate_output_format(format)
        df = active_data_source.get_trade_dates(start_date=start_date, end_date=end_date)
        return format_table_output(df, format=format, max_rows=limit,
                                   meta={"start_date": start_date or "默认", "end_date": end_date or "默认"})

    return run_tool_with_handling(action, context="get_trade_dates")


@app.tool()
def get_all_stock(date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """获取全部 A 股及指数列表，含交易状态。

    Args:
        date: 查询日期 'YYYY-MM-DD'，默认当天
        limit: 最大返回行数，默认 250
        format: 输出格式，默认 'markdown'

    Returns:
        包含股票代码和交易状态的表格（1=正常交易, 0=停牌）
    """
    def action():
        validate_output_format(format)
        df = active_data_source.get_all_stock(date=date)
        return format_table_output(df, format=format, max_rows=limit, meta={"as_of": date or "默认"})

    return run_tool_with_handling(action, context=f"get_all_stock:{date or 'default'}")


@app.tool()
def search_stocks(keyword: str, date: Optional[str] = None, limit: int = 50, format: str = "markdown") -> str:
    """按股票代码子串模糊搜索。

    Args:
        keyword: 代码子串，如 '600'、'000001'
        date: 查询日期 'YYYY-MM-DD'，默认当天
        limit: 最大返回行数，默认 50
        format: 输出格式，默认 'markdown'
    """
    def action():
        validate_output_format(format)
        validate_non_empty_str(keyword, "keyword")
        df = active_data_source.get_all_stock(date=date)
        if df is None or df.empty:
            return "(无可用数据)"
        filtered = df[df["code"].str.lower().str.contains(keyword.strip().lower(), na=False)]
        return format_table_output(filtered, format=format, max_rows=limit,
                                   meta={"keyword": keyword, "as_of": date or "当天"})

    return run_tool_with_handling(action, context=f"search_stocks:{keyword}")


@app.tool()
def get_suspensions(date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """获取指定日期的停牌股票列表。

    Args:
        date: 查询日期 'YYYY-MM-DD'，默认当天
        limit: 最大返回行数，默认 250
        format: 输出格式，默认 'markdown'
    """
    def action():
        validate_output_format(format)
        df = active_data_source.get_all_stock(date=date)
        if df is None or df.empty:
            return "(无可用数据)"
        if "tradeStatus" not in df.columns:
            raise ValueError("数据源返回结果中缺少 'tradeStatus' 列")
        suspended = df[df["tradeStatus"] == '0']
        return format_table_output(suspended, format=format, max_rows=limit,
                                   meta={"as_of": date or "当天", "total_suspended": int(suspended.shape[0])})

    return run_tool_with_handling(action, context=f"get_suspensions:{date or 'current'}")
