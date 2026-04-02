"""
综合分析工具模块。

提供个股综合分析报告的 MCP 工具，支持基本面、技术面及综合分析三种模式。
分析报告基于公开市场数据生成，仅供参考，不构成投资建议。
"""
from src.data_source import active_data_source
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.core.analysis import build_stock_analysis_report


@app.tool()
def get_stock_analysis(code: str, analysis_type: str = "fundamental") -> str:
    """生成个股数据分析报告（非投资建议）。

    Args:
        code: Baostock 格式股票代码，如 'sh.600000'
        analysis_type: 分析类型（'fundamental' 基本面 / 'technical' 技术面 / 'comprehensive' 综合）
    """
    return run_tool_with_handling(
        lambda: build_stock_analysis_report(active_data_source, code=code, analysis_type=analysis_type),
        context=f"get_stock_analysis:{code}:{analysis_type}",
    )
