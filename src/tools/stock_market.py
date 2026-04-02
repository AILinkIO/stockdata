"""
股票行情数据工具模块。

提供 A 股个股行情相关的 MCP 工具，包括：
- 历史 K 线数据（日/周/月/分钟级别）
- 股票基本信息查询
- 分红送转数据
- 复权因子数据
"""
from src.data_source import active_data_source
from src.formatting.markdown import format_table_output
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.services.validation import (
    validate_adjust_flag,
    validate_frequency,
    validate_output_format,
    validate_year,
    validate_year_type,
)


@app.tool()
def get_historical_k_data(
    code: str,
    start_date: str,
    end_date: str,
    frequency: str = "d",
    adjust_flag: str = "3",
    fields: list[str] | None = None,
    limit: int = 250,
    format: str = "markdown",
) -> str:
    """获取 A 股历史 K 线（OHLCV）数据。

    Args:
        code: Baostock 格式股票代码，如 'sh.600000'、'sz.000001'
        start_date: 起始日期，'YYYY-MM-DD' 格式
        end_date: 结束日期，'YYYY-MM-DD' 格式
        frequency: 数据频率（'d' 日 / 'w' 周 / 'm' 月 / '5'/'15'/'30'/'60' 分钟），默认 'd'
        adjust_flag: 复权类型（'1' 后复权 / '2' 前复权 / '3' 不复权），默认 '3'
        fields: 可选字段列表，为 None 时使用默认字段
        limit: 最大返回行数，默认 250
        format: 输出格式 'markdown' | 'json' | 'csv'，默认 'markdown'
    """
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
def get_stock_basic_info(code: str, fields: list[str] | None = None, format: str = "markdown") -> str:
    """获取股票基本信息（名称、行业、上市日期等）。

    Args:
        code: Baostock 格式股票代码，如 'sh.600000'
        fields: 可选字段过滤列表（如 ['code', 'code_name', 'industry']），为 None 时返回全部
        format: 输出格式 'markdown' | 'json' | 'csv'，默认 'markdown'
    """
    def action():
        validate_output_format(format)
        df = active_data_source.get_stock_basic_info(code=code, fields=fields)
        rows = df.shape[0] if df is not None else 0
        return format_table_output(df, format=format, max_rows=rows, meta={"code": code})

    return run_tool_with_handling(action, context=f"get_stock_basic_info:{code}")


@app.tool()
def get_dividend_data(code: str, year: str, year_type: str = "report", limit: int = 250, format: str = "markdown") -> str:
    """获取股票分红送转数据。

    Args:
        code: Baostock 格式股票代码
        year: 查询年份，如 '2023'
        year_type: 年份类型（'report' 预案公告年份 / 'operate' 除权除息年份），默认 'report'
        limit: 最大返回行数，默认 250
        format: 输出格式，默认 'markdown'
    """
    def action():
        validate_year(year)
        validate_year_type(year_type)
        validate_output_format(format)
        df = active_data_source.get_dividend_data(code=code, year=year, year_type=year_type)
        return format_table_output(df, format=format, max_rows=limit,
                                   meta={"code": code, "year": year, "year_type": year_type})

    return run_tool_with_handling(action, context=f"get_dividend_data:{code}:{year}")


@app.tool()
def get_adjust_factor_data(code: str, start_date: str, end_date: str, limit: int = 250, format: str = "markdown") -> str:
    """获取复权因子数据，用于计算前/后复权价格。

    Args:
        code: Baostock 格式股票代码
        start_date: 起始日期，'YYYY-MM-DD' 格式
        end_date: 结束日期，'YYYY-MM-DD' 格式
        limit: 最大返回行数，默认 250
        format: 输出格式，默认 'markdown'
    """
    def action():
        validate_output_format(format)
        df = active_data_source.get_adjust_factor_data(code=code, start_date=start_date, end_date=end_date)
        return format_table_output(df, format=format, max_rows=limit,
                                   meta={"code": code, "start_date": start_date, "end_date": end_date})

    return run_tool_with_handling(action, context=f"get_adjust_factor_data:{code}")
