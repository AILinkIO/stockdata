"""
财务报表数据工具模块。

提供上市公司季度/年度财务报告相关的 MCP 工具，包括：
- 盈利能力、营运能力、成长能力、偿债能力、现金流量、杜邦分析（按季度）
- 业绩快报 / 业绩预告（按日期范围）
- 综合财务指标（聚合6大类指标的便捷查询）
"""
from src.data_source import active_data_source
from src.formatting.markdown import format_table_output
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.services.validation import validate_output_format, validate_quarter, validate_year


def _fetch_quarterly(data_source_method, *, code: str, year: str, quarter: int,
                     dataset: str, limit: int, format: str) -> str:
    """季度财务数据的通用获取逻辑：校验 → 查询 → 格式化。"""
    validate_year(year)
    validate_quarter(quarter)
    validate_output_format(format)
    df = data_source_method(code=code, year=year, quarter=quarter)
    return format_table_output(df, format=format, max_rows=limit,
                               meta={"code": code, "year": year, "quarter": quarter, "dataset": dataset})


def _fetch_date_range(data_source_method, *, code: str, start_date: str, end_date: str,
                      dataset: str, limit: int, format: str) -> str:
    """日期范围财务数据的通用获取逻辑：校验 → 查询 → 格式化。"""
    validate_output_format(format)
    df = data_source_method(code=code, start_date=start_date, end_date=end_date)
    return format_table_output(df, format=format, max_rows=limit,
                               meta={"code": code, "start_date": start_date, "end_date": end_date, "dataset": dataset})


@app.tool()
def get_profit_data(code: str, year: str, quarter: int, limit: int = 250, format: str = "markdown") -> str:
    """获取季度盈利能力数据（ROE、净利率、毛利率等）。"""
    return run_tool_with_handling(
        lambda: _fetch_quarterly(active_data_source.get_profit_data,
                                 code=code, year=year, quarter=quarter,
                                 dataset="盈利能力", limit=limit, format=format),
        context=f"get_profit_data:{code}:{year}Q{quarter}",
    )


@app.tool()
def get_operation_data(code: str, year: str, quarter: int, limit: int = 250, format: str = "markdown") -> str:
    """获取季度营运能力数据（应收账款周转率、存货周转率等）。"""
    return run_tool_with_handling(
        lambda: _fetch_quarterly(active_data_source.get_operation_data,
                                 code=code, year=year, quarter=quarter,
                                 dataset="营运能力", limit=limit, format=format),
        context=f"get_operation_data:{code}:{year}Q{quarter}",
    )


@app.tool()
def get_growth_data(code: str, year: str, quarter: int, limit: int = 250, format: str = "markdown") -> str:
    """获取季度成长能力数据（净利润同比、总资产同比等）。"""
    return run_tool_with_handling(
        lambda: _fetch_quarterly(active_data_source.get_growth_data,
                                 code=code, year=year, quarter=quarter,
                                 dataset="成长能力", limit=limit, format=format),
        context=f"get_growth_data:{code}:{year}Q{quarter}",
    )


@app.tool()
def get_balance_data(code: str, year: str, quarter: int, limit: int = 250, format: str = "markdown") -> str:
    """获取季度偿债能力数据（流动比率、速动比率、资产负债率等）。"""
    return run_tool_with_handling(
        lambda: _fetch_quarterly(active_data_source.get_balance_data,
                                 code=code, year=year, quarter=quarter,
                                 dataset="偿债能力", limit=limit, format=format),
        context=f"get_balance_data:{code}:{year}Q{quarter}",
    )


@app.tool()
def get_cash_flow_data(code: str, year: str, quarter: int, limit: int = 250, format: str = "markdown") -> str:
    """获取季度现金流量数据。"""
    return run_tool_with_handling(
        lambda: _fetch_quarterly(active_data_source.get_cash_flow_data,
                                 code=code, year=year, quarter=quarter,
                                 dataset="现金流量", limit=limit, format=format),
        context=f"get_cash_flow_data:{code}:{year}Q{quarter}",
    )


@app.tool()
def get_dupont_data(code: str, year: str, quarter: int, limit: int = 250, format: str = "markdown") -> str:
    """获取季度杜邦分析数据。"""
    return run_tool_with_handling(
        lambda: _fetch_quarterly(active_data_source.get_dupont_data,
                                 code=code, year=year, quarter=quarter,
                                 dataset="杜邦分析", limit=limit, format=format),
        context=f"get_dupont_data:{code}:{year}Q{quarter}",
    )


@app.tool()
def get_performance_express_report(code: str, start_date: str, end_date: str, limit: int = 250, format: str = "markdown") -> str:
    """获取业绩快报（指定日期范围）。"""
    return run_tool_with_handling(
        lambda: _fetch_date_range(active_data_source.get_performance_express_report,
                                  code=code, start_date=start_date, end_date=end_date,
                                  dataset="业绩快报", limit=limit, format=format),
        context=f"get_performance_express_report:{code}:{start_date}-{end_date}",
    )


@app.tool()
def get_forecast_report(code: str, start_date: str, end_date: str, limit: int = 250, format: str = "markdown") -> str:
    """获取业绩预告（指定日期范围）。"""
    return run_tool_with_handling(
        lambda: _fetch_date_range(active_data_source.get_forecast_report,
                                  code=code, start_date=start_date, end_date=end_date,
                                  dataset="业绩预告", limit=limit, format=format),
        context=f"get_forecast_report:{code}:{start_date}-{end_date}",
    )


@app.tool()
def get_fina_indicator(code: str, start_date: str, end_date: str, limit: int = 250, format: str = "markdown") -> str:
    """获取综合财务指标，一次性聚合 6 大类 Baostock 数据。

    按季度返回，输入日期范围决定查询哪些季度。聚合数据来源：
    - 盈利能力: roeAvg, npMargin, gpMargin, epsTTM
    - 营运能力: NRTurnRatio, INVTurnRatio, CATurnRatio
    - 成长能力: YOYNI, YOYEquity, YOYAsset
    - 偿债能力: currentRatio, quickRatio, liabilityToAsset
    - 现金流量: CFOToOR, CFOToNP, CAToAsset
    - 杜邦分析: dupontROE, dupontAssetTurn, dupontPnitoni

    输出列名带前缀（profit_* / operation_* / growth_* / balance_* / cashflow_* / dupont_*）以区分来源。
    """
    return run_tool_with_handling(
        lambda: _fetch_date_range(active_data_source.get_fina_indicator,
                                  code=code, start_date=start_date, end_date=end_date,
                                  dataset="综合财务指标", limit=limit, format=format),
        context=f"get_fina_indicator:{code}:{start_date}-{end_date}",
    )
