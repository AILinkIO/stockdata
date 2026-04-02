"""
财务报表数据工具模块。

提供上市公司季度/年度财务报告相关的 MCP 工具，包括：
- 盈利能力数据（利润表）
- 营运能力数据
- 成长能力数据
- 偿债能力数据（资产负债表）
- 现金流量数据
- 杜邦分析数据
- 业绩快报 / 业绩预告
- 综合财务指标（聚合6大类指标的便捷查询）

所有工具按季度返回数据，输入日期范围决定查询哪些季度。
"""
import logging

from src.data_source import active_data_source
from src.formatting.markdown_formatter import format_table_output
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.services.validation import (
    validate_output_format,
    validate_quarter,
    validate_year,
)

logger = logging.getLogger(__name__)


def _fetch_quarterly(data_source_method, *, code: str, year: str, quarter: int,
                     dataset: str, limit: int, format: str) -> str:
    """季度财务数据的通用获取逻辑：校验 → 查询 → 格式化。"""
    validate_year(year)
    validate_quarter(quarter)
    validate_output_format(format)
    df = data_source_method(code=code, year=year, quarter=quarter)
    meta = {"code": code, "year": year, "quarter": quarter, "dataset": dataset}
    return format_table_output(df, format=format, max_rows=limit, meta=meta)


def _fetch_date_range(data_source_method, *, code: str, start_date: str, end_date: str,
                      dataset: str, limit: int, format: str) -> str:
    """日期范围财务数据的通用获取逻辑：校验 → 查询 → 格式化。"""
    validate_output_format(format)
    df = data_source_method(code=code, start_date=start_date, end_date=end_date)
    meta = {"code": code, "start_date": start_date, "end_date": end_date, "dataset": dataset}
    return format_table_output(df, format=format, max_rows=limit, meta=meta)


@app.tool()
def get_profit_data(code: str, year: str, quarter: int, limit: int = 250, format: str = "markdown") -> str:
    """Quarterly profitability data."""
    return run_tool_with_handling(
        lambda: _fetch_quarterly(active_data_source.get_profit_data,
                                 code=code, year=year, quarter=quarter,
                                 dataset="Profitability", limit=limit, format=format),
        context=f"get_profit_data:{code}:{year}Q{quarter}",
    )


@app.tool()
def get_operation_data(code: str, year: str, quarter: int, limit: int = 250, format: str = "markdown") -> str:
    """Quarterly operation capability data."""
    return run_tool_with_handling(
        lambda: _fetch_quarterly(active_data_source.get_operation_data,
                                 code=code, year=year, quarter=quarter,
                                 dataset="Operation Capability", limit=limit, format=format),
        context=f"get_operation_data:{code}:{year}Q{quarter}",
    )


@app.tool()
def get_growth_data(code: str, year: str, quarter: int, limit: int = 250, format: str = "markdown") -> str:
    """Quarterly growth capability data."""
    return run_tool_with_handling(
        lambda: _fetch_quarterly(active_data_source.get_growth_data,
                                 code=code, year=year, quarter=quarter,
                                 dataset="Growth", limit=limit, format=format),
        context=f"get_growth_data:{code}:{year}Q{quarter}",
    )


@app.tool()
def get_balance_data(code: str, year: str, quarter: int, limit: int = 250, format: str = "markdown") -> str:
    """Quarterly balance sheet data."""
    return run_tool_with_handling(
        lambda: _fetch_quarterly(active_data_source.get_balance_data,
                                 code=code, year=year, quarter=quarter,
                                 dataset="Balance Sheet", limit=limit, format=format),
        context=f"get_balance_data:{code}:{year}Q{quarter}",
    )


@app.tool()
def get_cash_flow_data(code: str, year: str, quarter: int, limit: int = 250, format: str = "markdown") -> str:
    """Quarterly cash flow data."""
    return run_tool_with_handling(
        lambda: _fetch_quarterly(active_data_source.get_cash_flow_data,
                                 code=code, year=year, quarter=quarter,
                                 dataset="Cash Flow", limit=limit, format=format),
        context=f"get_cash_flow_data:{code}:{year}Q{quarter}",
    )


@app.tool()
def get_dupont_data(code: str, year: str, quarter: int, limit: int = 250, format: str = "markdown") -> str:
    """Quarterly Dupont analysis data."""
    return run_tool_with_handling(
        lambda: _fetch_quarterly(active_data_source.get_dupont_data,
                                 code=code, year=year, quarter=quarter,
                                 dataset="Dupont", limit=limit, format=format),
        context=f"get_dupont_data:{code}:{year}Q{quarter}",
    )


@app.tool()
def get_performance_express_report(code: str, start_date: str, end_date: str, limit: int = 250, format: str = "markdown") -> str:
    """Performance express report within date range."""
    return run_tool_with_handling(
        lambda: _fetch_date_range(active_data_source.get_performance_express_report,
                                  code=code, start_date=start_date, end_date=end_date,
                                  dataset="Performance Express", limit=limit, format=format),
        context=f"get_performance_express_report:{code}:{start_date}-{end_date}",
    )


@app.tool()
def get_forecast_report(code: str, start_date: str, end_date: str, limit: int = 250, format: str = "markdown") -> str:
    """Earnings forecast report within date range."""
    return run_tool_with_handling(
        lambda: _fetch_date_range(active_data_source.get_forecast_report,
                                  code=code, start_date=start_date, end_date=end_date,
                                  dataset="Forecast", limit=limit, format=format),
        context=f"get_forecast_report:{code}:{start_date}-{end_date}",
    )


@app.tool()
def get_fina_indicator(code: str, start_date: str, end_date: str, limit: int = 250, format: str = "markdown") -> str:
    """
    Aggregated financial indicators from 6 Baostock APIs into one convenient query.

    **Data is returned by QUARTER** (Q1, Q2, Q3, Q4) based on financial report dates.
    Input date range determines which quarters to fetch.

    Combines data from:
    - 盈利能力 (Profitability): roeAvg, npMargin, gpMargin, epsTTM
    - 营运能力 (Operation): NRTurnRatio, INVTurnRatio, CATurnRatio
    - 成长能力 (Growth): YOYNI, YOYEquity, YOYAsset
    - 偿债能力 (Solvency): currentRatio, quickRatio, liabilityToAsset
    - 现金流量 (Cash Flow): CFOToOR, CFOToNP, CAToAsset
    - 杜邦分析 (DuPont): dupontROE, dupontAssetTurn, dupontPnitoni

    Output columns include prefixes: profit_*, operation_*, growth_*,
    balance_*, cashflow_*, dupont_* to distinguish data sources.
    """
    return run_tool_with_handling(
        lambda: _fetch_date_range(active_data_source.get_fina_indicator,
                                  code=code, start_date=start_date, end_date=end_date,
                                  dataset="Financial Indicators", limit=limit, format=format),
        context=f"get_fina_indicator:{code}:{start_date}-{end_date}",
    )
