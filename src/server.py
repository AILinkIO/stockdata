"""
FastMCP 应用实例模块。

创建全局唯一的 FastMCP 应用，供所有工具模块直接 import 并通过
@app.tool() 装饰器注册工具。
"""
from datetime import datetime

from fastmcp import FastMCP

current_date = datetime.now().strftime("%Y-%m-%d")

app = FastMCP(
    name="a_share_data_provider",
    instructions=f"""今天是{current_date}。提供中国A股市场数据分析工具。此服务提供客观数据分析，用户需自行做出投资决策。数据分析基于公开市场信息，不构成投资建议，仅供参考。

⚠️ 重要说明:
1. 最新交易日不一定是今天，需要从 get_latest_trading_date() 获取
2. 请始终使用 get_latest_trading_date() 工具获取实际当前最近的交易日，不要依赖训练数据中的日期认知
3. 当分析"最近"或"近期"市场情况时，必须首先调用 get_market_analysis_timeframe() 工具确定实际的分析时间范围
4. 任何涉及日期的分析必须基于工具返回的实际数据，不得使用过时或假设的日期
""",
)
