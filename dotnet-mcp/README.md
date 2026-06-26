# dotnet-mcp

C# MCP 服务，同时是 **stockdata 的唯一 PostgreSQL 属主**：直读/落盘 PG、经
[fetch 微服务](../server/)（HTTP）编排 baostock 抓取、后台常驻 `SyncDrainer` 全量续传，
并内置 TA-Lib 指标分析。共 **44 个工具**（38 数据 + 6 指标）。

- 技术栈：.NET 10 (LTS) / ModelContextProtocol SDK (Streamable HTTP) / EF Core (Npgsql) /
  TALib.NETCore（纯托管）
- 监听：`http://0.0.0.0:8000`，MCP 端点 `/mcp`，健康检查 `/healthz`，同步控制面 `/sync/*`
- 架构：**不直连 baostock**——抓取统一走 fetch 微服务（submit+poll，`high/low` 优先级），
  数据落盘 PG、serving 直读 PG。详见仓库根 [README.md](../README.md) 的「数据流」节。

## 数据/抓取模型（`ServeFromPgOnly=true`）

- **读路径** — `KlineReadService` 等读到一个 code：懒登记进 `synced_stock` + `stock_sync_task`
  （不触 baostock）→ 用 `high` 优先在 fetch 队列插队、按 `ReadFetchBudgetSeconds`（默认 30s）有界抓取
  （超预算/失败则吞掉异常、回退读 PG 现状，见 `Data/ReadFetch.cs`）。
- **后台同步** — 常驻 `SyncDrainer`（`BackgroundService`，**唯一串行驱动 baostock 的 worker**）从
  `stock_sync_task` 取 due 任务逐票 `low` 优先全量续传，并按 `MarketRefreshSeconds` 自维护市场级数据。
- **halt 自愈** — baostock 拉黑时 fetch 进入 halted；`FetchHaltMonitor` 轮询感知、冷却后自动 `/restart`。
- **覆盖度判定** — `Coverage`/`Decision` 纯函数据 `data_watermark` 算出需抓区间，Fresh 则直读 PG。

> `PipelineEnabled=false` 时上述全部不注册：MCP 工具回退旧 REST（`ApiBase`），`/sync/*` 返回 503。

## 启动

```bash
# 本地开发（需 .NET 10 SDK；管线开启还需 PG + Redis + fetch 在跑）
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

环境变量形如 `StockData__<Key>`（compose 注入；`PgDsn` 取 `STOCKDATA_PG_DSN`）。

| 配置键 | 默认值 | 说明 |
|---|---|---|
| `StockData:PipelineEnabled` | `false` | dotnet 数据管线总开关（落盘/同步/控制面/Drainer） |
| `StockData:ServeFromPgOnly` | `false` | 新抓取模型：读纯走 PG + 懒登记 + 定向高优先有界抓 |
| `StockData:PgDsn`（`STOCKDATA_PG_DSN`） | — | dotnet 连 PG 的 DSN |
| `StockData:FetchBase` | `http://127.0.0.1:8090` | fetch 微服务地址 |
| `StockData:ApiBase` | `http://127.0.0.1:8080` | 旧 REST 兜底（管线关闭时用） |
| `StockData:ReadFetchBudgetSeconds` | `30` | 读路径有界抓取预算（秒） |
| `StockData:Sync:DrainIdleSeconds` | `10` | Drainer 空队列轮询间隔 |
| `StockData:Sync:MarketRefreshSeconds` | `3600` | 市场级数据自维护间隔 |
| `StockData:Sync:StaleAfterHours` | `20` | `/sync/refresh` 判过期重排的小时数 |
| `StockData:FetchHaltPollSeconds` / `FetchRestartCooldownSeconds` | `60` / `600` | halt 监视轮询 / 自动 restart 冷却 |

弹性策略（REST typed client）：单次尝试超时 150s、总预算 360s、502/504 重试 2 次。

## 同步控制面（cron 调，秒回、不抓 baostock）

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/sync/refresh` | 把完成早于 `StaleAfterHours` 的 done 任务重置 pending，交 Drainer 后台消费 |
| `GET`  | `/sync/status`  | 同步进度（已纳管票数 + 按状态计数） |

单票同步无独立端点：由 MCP 读路径懒登记触发；市场级数据由 Drainer 自动维护。

## 工具清单（44）

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

### 指标分析（TA-Lib，6 个）

K 线先经 `KlineReadService`（PG），缺口按上述模型抓取；lookback 自动预热（向前多取并裁剪，避免序列头部 NaN）。
每个工具 `output=series`（逐日序列）/`latest`（最新值），`adjust_flag`（2前复权默认/1后复权/3不复权）、
`frequency`（d/w/m）可调。

| 工具 | 说明 |
|---|---|
| `get_rsi` | 相对强弱指数（默认 14），latest 附超买/超卖判断 |
| `get_obv` | 能量潮（量价方向累积） |
| `get_cci` | 双周期 CCI 动量系统（CCI55 快线 + CCI144 慢线 + DIFF），输出 zone 强弱分区 |
| `get_dual_ma` | 双均线趋势系统（EMA，默认 20/50）：fast/slow/spread/cross(金叉死叉)/trend |
| `get_ma_alignment` | 均线多头排列检测（默认 SMA5/10/20/60），输出 alignment + signal（形成/破坏） |
| `get_vegas_channel` | Vegas 通道（EMA12/21 + 144/169 + 233 + 576/676），输出 zone 区域判断 |

## 目录结构

```
src/StockData.Mcp/
  Program.cs              # MCP host + DI + /healthz + /sync/* 端点
  Coverage/               # 覆盖度判定（水位 → 需抓区间，纯函数）
  Data/                   # PG 属主：EF 实体/迁移、各数据集 Read/Pipeline Service、
                          #   SyncDrainer（常驻消费）、SyncRegistry（懒登记）、ReadFetch（有界抓取）
  Fetching/               # fetch 微服务客户端、high/low 优先级、halt 监视
  Indicators/             # K 线序列加载、TA-Lib 指标引擎
  StockDataClient/        # 旧 REST 客户端（管线关闭时兜底）
  Tools/                  # 44 个 MCP 工具（按领域分文件）
tests/StockData.Mcp.Tests # xUnit（覆盖度/解析/管线 E2E/指标/Live 验证）
```
