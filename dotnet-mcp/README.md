# dotnet-mcp

C# MCP 服务：将 [server/](../server/) REST API（baostock 数据）透传为 MCP 工具，
并内置 TA-Lib 指标分析能力。共 **43 个工具**（38 透传 + 5 指标分析）。

- 技术栈：.NET 10 (LTS) / ModelContextProtocol SDK (Streamable HTTP) / TALib.NETCore（纯托管）
- 监听：`http://0.0.0.0:8000`，MCP 端点 `/mcp`，健康检查 `/healthz`
- 架构：不直连 baostock；所有数据（含指标计算用的前复权 K 线）均来自 server REST API（默认 `http://127.0.0.1:8080`）。透传组直接转发 JSON 文本，不经 double 转换，数值保持原样。

## 启动

```bash
# 本地开发（需 .NET 10 SDK）
dotnet run --project src/StockData.Mcp

# 测试
dotnet test StockData.Mcp.slnx

# 容器（随全栈一起，见仓库根目录 compose.yaml 的 mcp 服务）
cd .. && ./up.sh          # 或只拉起本服务：./up.sh mcp
```

MCP 客户端配置示例（Streamable HTTP）：

```json
{ "mcpServers": { "stockdata": { "type": "http", "url": "http://127.0.0.1:8000/mcp" } } }
```

## 配置

| 配置键 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `StockData:ApiBase` | `StockData__ApiBase` | `http://127.0.0.1:8080` | server REST API 地址 |

弹性策略：单次尝试超时 100s（覆盖 API 读穿透 60s 等待）、总预算 240s、502/504 重试 2 次。

## 工具清单

### 行情（MarketDataTools）

| 工具 | 说明 |
|---|---|
| `get_historical_k_data` | 历史 K 线（日/周/月/分钟线自动分流，支持复权） |
| `get_stock_basic_info` | 证券基本资料 |
| `get_dividend_data` | 分红派息 |
| `get_adjust_factor_data` | 复权因子 |
| `get_stock_analysis` | 个股综合分析数据 |

### 财报（FinancialTools）

| 工具 | 说明 |
|---|---|
| `get_profit_data` | 盈利能力 |
| `get_operation_data` | 营运能力 |
| `get_growth_data` | 成长能力 |
| `get_balance_data` | 偿债能力 |
| `get_cash_flow_data` | 现金流量 |
| `get_dupont_data` | 杜邦指数 |
| `get_performance_express_report` | 业绩快报 |
| `get_forecast_report` | 业绩预告 |
| `get_fina_indicator` | 财务指标汇总 |

### 指数与行业（IndexTools）

| 工具 | 说明 |
|---|---|
| `get_index_constituents` | 指数成份股 |
| `get_sz50_stocks` / `get_hs300_stocks` / `get_zz500_stocks` | 上证50 / 沪深300 / 中证500 成份股 |
| `get_stock_industry` | 个股行业分类 |
| `list_industries` | 行业列表 |
| `get_industry_members` | 行业成员 |

### 市场与宏观（MarketOverviewTools / MacroTools）

| 工具 | 说明 |
|---|---|
| `get_trade_dates` | 交易日历 |
| `get_all_stock` | 全市场证券列表 |
| `search_stocks` | 按代码/名称搜索 |
| `get_suspensions` | 停复牌 |
| `get_deposit_rate_data` / `get_loan_rate_data` | 存款 / 贷款利率 |
| `get_required_reserve_ratio_data` | 存款准备金率 |
| `get_money_supply_data_month` / `get_money_supply_data_year` | 货币供应量（月/年） |

### 日期与工具（DateTools / UtilTools）

| 工具 | 说明 |
|---|---|
| `get_latest_trading_date` | 最近交易日 |
| `is_trading_day` | 是否交易日 |
| `previous_trading_day` / `next_trading_day` | 前/后一个交易日 |
| `get_last_n_trading_days` | 最近 N 个交易日 |
| `get_recent_trading_range` | 最近交易区间 |
| `normalize_stock_code` / `normalize_index_code` | 证券/指数代码规范化 |

### 指标分析（IndicatorTools，TA-Lib）

| 工具 | 说明 |
|---|---|
| `calc_indicators` | MA/EMA/MACD/RSI/KDJ/BOLL/ATR/OBV/CCI，参数可调，返回序列或最新值；lookback 自动预热（向前多取并裁剪，避免序列头部 NaN） |
| `detect_candlestick_patterns` | 15 个常用 K 线形态识别，返回命中日期与方向 |
| `technical_summary` | 均线排列/金叉死叉/超买超卖/布林位置的结构化结论 |
| `compare_indicators` | 多标的同指标横向对比 |
| `get_ma_alignment` | 均线多头排列检测：默认 SMA5/10/20/60，可自定义周期，输出 alignment（多头/空头/未排列）+ signal（形成/破坏） |

KDJ 口径：TA-Lib STOCH 仅给出 K/D，J 值按 `3K - 2D` 推导，与主流行情软件一致（C6 已对照验证）。

## 目录结构

```
src/StockData.Mcp/
  Program.cs              # MCP host + typed client 注册
  StockDataClient/        # REST 客户端（弹性管道、JSON 透传）
  Tools/                  # 43 个 MCP 工具（按领域分文件）
  Indicators/             # K 线序列加载、指标引擎、形态引擎
tests/StockData.Mcp.Tests # xUnit（lookback/参数校验/错误路径）
```
