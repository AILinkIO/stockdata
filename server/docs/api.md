# API 参考

> 在线交互文档：服务运行后访问 <http://localhost:8080/docs>（OpenAPI 自动生成，参数定义以其为准）。
> 本文档侧重接口清单、全局约定与行为语义。数据新鲜度与抓取行为见 [data-lifecycle.md](data-lifecycle.md)。

## 全局约定

- **Base URL**：`http://<host>:8080`，所有业务接口在 `/api/v1` 前缀下，均为 `GET`（除任务提交）。
  （8000 是旧 MCP 服务的端口，REST 服务自 2026-06-11 起使用 8080，避免混淆。）
- **股票代码宽松格式**：所有 `code` 参数支持 `sh.600000` / `600000` / `600000.SH` / `SH600000` 等常见写法，服务端自动标准化（6 开头视为上交所）。
- **日期格式**：`YYYY-MM-DD`。
- **读穿透**：数据接口在库中缺数据/数据过期时自动投递抓取任务并等待（默认最长 60s）。
  首次查询某标的可能需要数秒~数十秒（全史回填），之后毫秒级。
- **响应**：JSON（数值字段为精确小数序列化；财报指标保留 baostock 原始字段名）。
  `analysis` 接口返回 `text/plain` 的 Markdown。

### 错误语义

| HTTP | 含义 | 处理建议 |
|---|---|---|
| 404 | 查询范围内无数据 | 检查代码/日期范围 |
| 422 | 参数校验失败 | 见响应 detail |
| 502 | 数据源（baostock）故障，重试已耗尽 | 稍后重试 |
| 504 | 等待抓取超时，任务仍在后台执行 | 稍后重试同一请求（届时已落库，直接命中） |

## 股票行情 `/api/v1/stocks/{code}/...`

| 接口 | 参数 | 说明 |
|---|---|---|
| `GET .../kline` | `start_date`*、`end_date`*、`frequency`=d/w/m（默认 d）、`adjust_flag`=1/2/3（默认 3） | 日/周/月 K 线。1=后复权、2=前复权、3=不复权；复权价读取时由因子计算（见 data-lifecycle.md） |
| `GET .../kline-minute` | `start_date`*、`end_date`*、`frequency`=5/15/30/60（默认 30） | 分钟 K 线，仅不复权；回填起点受 `STOCKDATA_MINUTE_BACKFILL_START` 限制 |
| `GET .../basic` | — | 基本信息：名称、上市/退市日、类型、状态 |
| `GET .../dividends` | `year`*、`year_type`=report/operate（默认 report） | 分红送转明细 |
| `GET .../adjust-factors` | `start_date`*、`end_date`* | 复权因子，每个除权除息事件一行 |
| `GET .../analysis` | `analysis_type`=fundamental/technical/comprehensive（默认 comprehensive） | 个股分析报告（Markdown 文本） |

带 `*` 为必填，下同。

```bash
curl "localhost:8080/api/v1/stocks/600000/kline?start_date=2024-01-01&end_date=2024-12-31&adjust_flag=2"
```

## 财务报表 `/api/v1/stocks/{code}/financials/...`

| 接口 | 参数 | 说明 |
|---|---|---|
| `GET .../{report_type}` | 路径 `report_type`=profit/operation/growth/balance/cash_flow/dupont；`year`*、`quarter`*(1-4) | 单季度财报；指标在 `metrics` 字段（baostock 原始字段名），`stat_date`=报告期、`pub_date`=披露日 |
| `GET .../indicator` | `start_date`*、`end_date`* | 综合财务指标：范围内每个报告期一行，六类指标合并、字段加类别前缀（如 `profit_roeAvg`） |
| `GET .../express` | `start_date`*、`end_date`* | 业绩快报；**按披露日期过滤**（报告期在范围外但披露日在范围内的也返回） |
| `GET .../forecast` | `start_date`*、`end_date`* | 业绩预告；同上按披露日期过滤 |

## 指数与行业

| 接口 | 参数 | 说明 |
|---|---|---|
| `GET /api/v1/indices/{index_code}/constituents` | 路径 `index_code`=sz50/hs300/zz500；`snap_date`（缺省=最新交易日） | 指数成分股 |
| `GET /api/v1/industries` | `snap_date`（缺省=最新交易日）、`code`（可选，过滤单只） | 申万行业分类 |

## 市场概览

| 接口 | 参数 | 说明 |
|---|---|---|
| `GET /api/v1/market/trade-calendar` | `start_date`*、`end_date`* | 交易日历（可查未来年度） |
| `GET /api/v1/market/stocks` | `snap_date`（可选） | 全部股票列表快照。缺省取最新交易日；当日列表盘中未发布时自动回退前一交易日（最多 3 个） |

## 宏观经济 `/api/v1/macro/...`

| 接口 | 参数 | 说明 |
|---|---|---|
| `GET .../deposit-rate` | `start_date`、`end_date`（缺省回看 10 年） | 基准存款利率 |
| `GET .../loan-rate` | 同上 | 基准贷款利率 |
| `GET .../rrr` | 同上 | 存款准备金率（大型/中小型机构） |
| `GET .../money-supply/month` | 同上 | 月度 M0/M1/M2 余额与同比环比 |
| `GET .../money-supply/year` | `start_year`、`end_year`（缺省近 10 年） | 年度 M0/M1/M2 年末余额 |

## 日期工具 `/api/v1/dates/...`

| 接口 | 参数 | 说明 |
|---|---|---|
| `GET .../latest-trading-day` | — | 最近交易日（今天是则返回今天） |
| `GET .../is-trading-day` | `date`* | 交易日判断 |
| `GET .../previous-trading-day` | `date`* | 之前最近的交易日 |
| `GET .../next-trading-day` | `date`* | 之后最近的交易日 |
| `GET .../last-trading-days` | `days`（默认 10，≤250） | 最近 N 个交易日列表 |

## 工具 `/api/v1/utils/...`

| 接口 | 参数 | 说明 |
|---|---|---|
| `GET .../normalize-code` | `code`* | 股票代码标准化 → `sh.600000` |
| `GET .../normalize-index-code` | `code`* | 指数代码/别名标准化（CSI300/HS300/000300 → `sh.000300`） |

## 任务管理 `/api/v1/tasks/...`

异步批量回填——不占用数据接口的同步等待，适合大范围预热：

```bash
# 提交（202）
curl -X POST localhost:8080/api/v1/tasks/backfill \
  -H "Content-Type: application/json" \
  -d '{"task": "fetcher.fetch_kline",
       "params": {"code": "sh.600000", "start_date": "2020-01-01",
                  "end_date": "2024-12-31", "frequency": "d"}}'
# → {"task_id": 42, "celery_task_id": "..."}；同参数任务已在队列时 task_id 为 null

# 轮询状态
curl localhost:8080/api/v1/tasks/42
# → {"status": "pending|running|succeeded|failed", "error": ..., "started_at": ...}
```

`task` 可选值（12 种，参数与 fetcher 任务签名一致）：
`fetcher.fetch_kline`、`fetch_kline_minute`、`fetch_adjust_factor`、`fetch_stock_basic`、
`fetch_dividend`、`fetch_financial_report`、`fetch_performance_report`、
`fetch_trade_calendar`、`fetch_stock_list`、`fetch_index_constituent`、
`fetch_industry`、`fetch_macro`。

## 运维

| 接口 | 说明 |
|---|---|
| `GET /healthz` | 健康检查（compose 健康探针使用） |
