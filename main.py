"""
A股数据 MCP 服务器入口文件。

基于 FastMCP 框架，通过 Streamable HTTP 传输协议对外暴露一组中国 A 股市场数据工具。
底层数据来源通过 src.data_source 模块提供（当前实现为 Baostock），便于后续替换。

启动方式:
    uv run python main.py

服务将在 http://0.0.0.0:8000/mcp 上以 streamable-http 模式运行。
"""

import logging

from src.server import app, current_date

# --- 防御性补丁：防止客户端取消工具调用时的竞态崩溃 ---
# mcp SDK 1.26.0 已知缺陷：当客户端发送 CancelledNotification 取消正在执行的
# 工具调用时，cancel() 与 respond() 之间存在竞态条件，导致 AssertionError 崩溃。
# https://github.com/modelcontextprotocol/python-sdk/issues/1152
import mcp.shared.session

_orig_respond = mcp.shared.session.RequestResponder.respond


async def _safe_respond(self, response):
    if self._completed:
        return
    return await _orig_respond(self, response)


mcp.shared.session.RequestResponder.respond = _safe_respond
# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- 导入各工具模块，模块加载时自动通过 @app.tool() 完成注册 ---
import src.tools.stock_market  # noqa: F401  K线、基本信息、分红、复权因子
import src.tools.financial_reports  # noqa: F401  利润、营运、成长、资产负债、现金流、杜邦分析等
import src.tools.indices  # noqa: F401  上证50、沪深300、中证500成分股及行业分类
import src.tools.market_overview  # noqa: F401  交易日历、全部股票列表、停牌查询
import src.tools.macroeconomic  # noqa: F401  存贷款利率、存款准备金率、货币供应量
import src.tools.date_utils  # noqa: F401  最新交易日、交易日判断、日期范围
import src.tools.analysis  # noqa: F401  基本面/技术面/综合分析报告
import src.tools.helpers  # noqa: F401  股票代码标准化、常量查询

# --- 服务器启动入口 ---
if __name__ == "__main__":
    logger.info(
        f"正在启动 A股 MCP 服务器 (streamable-http)... 当前日期: {current_date}"
    )
    # 以 streamable-http 模式启动，监听所有网络接口的 8000 端口
    # MCP 端点地址: http://0.0.0.0:8000/mcp
    app.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=8000,
        uvicorn_config={"timeout_graceful_shutdown": 0},
    )
