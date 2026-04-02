"""
宏观经济数据工具模块。

提供中国宏观经济指标相关的 MCP 工具，包括：
- 基准存款利率
- 基准贷款利率
- 存款准备金率
- 货币供应量（月度 M0/M1/M2）
- 货币供应量（年度余额）
"""
from typing import Optional

from src.data_source import active_data_source
from src.formatting.markdown import format_table_output
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.services.validation import validate_output_format, validate_year_type_reserve


def _fetch_macro(data_source_method, *, dataset: str, start_date: Optional[str],
                 end_date: Optional[str], limit: int, format: str, **extra) -> str:
    """宏观经济数据的通用获取逻辑：校验 → 查询 → 格式化。"""
    validate_output_format(format)
    df = data_source_method(start_date=start_date, end_date=end_date, **extra)
    meta = {"dataset": dataset, "start_date": start_date, "end_date": end_date, **extra}
    return format_table_output(df, format=format, max_rows=limit, meta=meta)


@app.tool()
def get_deposit_rate_data(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """获取基准存款利率数据。"""
    return run_tool_with_handling(
        lambda: _fetch_macro(active_data_source.get_deposit_rate_data,
                             dataset="基准存款利率", start_date=start_date, end_date=end_date,
                             limit=limit, format=format),
        context="get_deposit_rate_data",
    )


@app.tool()
def get_loan_rate_data(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """获取基准贷款利率数据。"""
    return run_tool_with_handling(
        lambda: _fetch_macro(active_data_source.get_loan_rate_data,
                             dataset="基准贷款利率", start_date=start_date, end_date=end_date,
                             limit=limit, format=format),
        context="get_loan_rate_data",
    )


@app.tool()
def get_required_reserve_ratio_data(start_date: Optional[str] = None, end_date: Optional[str] = None, year_type: str = '0', limit: int = 250, format: str = "markdown") -> str:
    """获取存款准备金率数据。

    Args:
        year_type: 年份类型（'0' 全部 / '1' 大型金融机构 / '2' 中小型金融机构），默认 '0'
    """
    def action():
        validate_output_format(format)
        validate_year_type_reserve(year_type)
        df = active_data_source.get_required_reserve_ratio_data(
            start_date=start_date, end_date=end_date, year_type=year_type)
        return format_table_output(df, format=format, max_rows=limit,
                                   meta={"dataset": "存款准备金率", "start_date": start_date,
                                         "end_date": end_date, "year_type": year_type})

    return run_tool_with_handling(action, context="get_required_reserve_ratio_data")


@app.tool()
def get_money_supply_data_month(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """获取月度货币供应量数据（M0/M1/M2）。"""
    return run_tool_with_handling(
        lambda: _fetch_macro(active_data_source.get_money_supply_data_month,
                             dataset="月度货币供应量", start_date=start_date, end_date=end_date,
                             limit=limit, format=format),
        context="get_money_supply_data_month",
    )


@app.tool()
def get_money_supply_data_year(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """获取年度货币供应量数据（年末余额）。"""
    return run_tool_with_handling(
        lambda: _fetch_macro(active_data_source.get_money_supply_data_year,
                             dataset="年度货币供应量", start_date=start_date, end_date=end_date,
                             limit=limit, format=format),
        context="get_money_supply_data_year",
    )
