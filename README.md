# stockdata

中国 A 股市场数据 MCP 服务器，基于 [FastMCP](https://github.com/PrefectHQ/fastmcp) 框架，通过 Streamable HTTP 协议对外提供数据查询工具。底层数据来源为 [Baostock](http://baostock.com/)。

## 功能

- **股票行情** — K 线（日/周/月/分钟级）、基本信息、分红送转、复权因子
- **财务报表** — 盈利能力、营运能力、成长能力、偿债能力、现金流、杜邦分析、业绩快报/预告、综合财务指标
- **指数与行业** — 上证 50、沪深 300、中证 500 成分股，行业分类
- **市场概览** — 交易日历、全部股票列表、停牌查询
- **宏观经济** — 存贷款利率、存款准备金率、货币供应量
- **日期工具** — 最新交易日、交易日判断、日期范围计算
- **分析报告** — 基本面 / 技术面 / 综合分析

## 快速开始

需要 Python 3.12+，使用 [uv](https://docs.astral.sh/uv/) 管理依赖。

```bash
# 安装依赖
uv sync

# 启动服务器
uv run python main.py
```

服务将在 `http://0.0.0.0:8000/mcp` 上以 Streamable HTTP 模式运行。

## 项目结构

```
src/
├── providers/       # 数据源层
│   ├── interface.py     # 抽象接口与异常定义
│   ├── baostock.py      # Baostock 实现
│   ├── context.py       # 会话管理（持久连接、自动重连）
│   └── cache.py         # diskcache 缓存代理
├── tools/           # MCP 工具定义
│   ├── stock_market.py      # K 线、基本信息、分红、复权因子
│   ├── financial_reports.py # 财务报表
│   ├── indices.py           # 指数与行业
│   ├── market_overview.py   # 市场概览
│   ├── macroeconomic.py     # 宏观经济
│   ├── date_utils.py        # 日期工具
│   ├── analysis.py          # 分析报告
│   └── helpers.py           # 股票代码标准化、常量查询
├── services/        # 业务逻辑
├── core/            # 核心模块
├── formatting/      # 输出格式化
├── server.py        # FastMCP 应用实例
└── data_source.py   # 数据源入口
```

## MCP 客户端配置

```json
{
  "mcpServers": {
    "stockdata": {
      "type": "http",
      "url": "http://localhost:8000/mcp"
    }
  }
}
```
