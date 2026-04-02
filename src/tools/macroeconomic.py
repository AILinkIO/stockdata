"""
宏观经济数据工具模块。

提供中国宏观经济指标相关的 MCP 工具，包括：
- 基准存款利率
- 基准贷款利率
- 存款准备金率
- 货币供应量（月度 M0/M1/M2）
- 货币供应量（年度余额）
"""
import logging
from typing import Optional

from src.data_source import active_data_source
from src.formatting.markdown_formatter import format_table_output
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.services.validation import validate_output_format, validate_year_type_reserve

logger = logging.getLogger(__name__)


def _fetch_macro(data_source_method, *, dataset: str, start_date: Optional[str],
                 end_date: Optional[str], limit: int, format: str, **extra) -> str:
    """宏观经济数据的通用获取逻辑：校验 → 查询 → 格式化。"""
    validate_output_format(format)
    df = data_source_method(start_date=start_date, end_date=end_date, **extra)
    meta = {"dataset": dataset, "start_date": start_date, "end_date": end_date}
    meta.update(extra)
    return format_table_output(df, format=format, max_rows=limit, meta=meta)


@app.tool()
def get_deposit_rate_data(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """Benchmark deposit rates."""
    return run_tool_with_handling(
        lambda: _fetch_macro(active_data_source.get_deposit_rate_data,
                             dataset="deposit_rate", start_date=start_date, end_date=end_date,
                             limit=limit, format=format),
        context="get_deposit_rate_data",
    )


@app.tool()
def get_loan_rate_data(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """Benchmark loan rates."""
    return run_tool_with_handling(
        lambda: _fetch_macro(active_data_source.get_loan_rate_data,
                             dataset="loan_rate", start_date=start_date, end_date=end_date,
                             limit=limit, format=format),
        context="get_loan_rate_data",
    )


@app.tool()
def get_required_reserve_ratio_data(start_date: Optional[str] = None, end_date: Optional[str] = None, year_type: str = '0', limit: int = 250, format: str = "markdown") -> str:
    """Required reserve ratio data."""
    def action():
        validate_output_format(format)
        validate_year_type_reserve(year_type)
        df = active_data_source.get_required_reserve_ratio_data(
            start_date=start_date, end_date=end_date, year_type=year_type)
        meta = {"dataset": "required_reserve_ratio", "start_date": start_date,
                "end_date": end_date, "year_type": year_type}
        return format_table_output(df, format=format, max_rows=limit, meta=meta)

    return run_tool_with_handling(action, context="get_required_reserve_ratio_data")


@app.tool()
def get_money_supply_data_month(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """Monthly money supply data."""
    return run_tool_with_handling(
        lambda: _fetch_macro(active_data_source.get_money_supply_data_month,
                             dataset="money_supply_month", start_date=start_date, end_date=end_date,
                             limit=limit, format=format),
        context="get_money_supply_data_month",
    )


@app.tool()
def get_money_supply_data_year(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """Yearly money supply data."""
    return run_tool_with_handling(
        lambda: _fetch_macro(active_data_source.get_money_supply_data_year,
                             dataset="money_supply_year", start_date=start_date, end_date=end_date,
                             limit=limit, format=format),
        context="get_money_supply_data_year",
    )
