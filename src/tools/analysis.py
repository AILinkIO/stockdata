"""
综合分析工具模块。

提供个股综合分析报告的 MCP 工具，支持基本面、技术面及综合分析三种模式。
分析报告基于公开市场数据生成，仅供参考，不构成投资建议。
"""
import logging

from src.data_source import active_data_source
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.core.analysis import build_stock_analysis_report

logger = logging.getLogger(__name__)


@app.tool()
def get_stock_analysis(code: str, analysis_type: str = "fundamental") -> str:
    """
    提供基于数据的股票分析报告，而非投资建议。

    Args:
        code: 股票代码，如'sh.600000'
        analysis_type: 'fundamental'|'technical'|'comprehensive'
    """
    logger.info(f"Tool 'get_stock_analysis' called for {code}, type={analysis_type}")
    return run_tool_with_handling(
        lambda: build_stock_analysis_report(active_data_source, code=code, analysis_type=analysis_type),
        context=f"get_stock_analysis:{code}:{analysis_type}",
    )
