# stockdata

A 股数据同步与展示：纯 Python 单服务。

- **一个常驻进程**（NiceGUI）承载全部功能：
  - Web 页面：`/` 关注列表管理、`/chart/{code}` K 线图、`/sync` 同步仪表盘；
  - REST API：`/api/sync/*`（启动/停止/状态/清熔断）；
  - **唯一的 baostock 同步 worker 线程**（严格单进程 + 单线程，绝不并发）。
- **CLI 是薄客户端**：`stockdata sync run --tui/--plain` 通过 HTTP 启动任务并跟踪进度，
  与 Web `/sync` 页看的是同一份进度状态。
- 数据落 PostgreSQL（物理机服务）；Redis 已不再需要。

## 架构

```
                 ┌──────────────────────── app 容器（唯一进程）───────────────────────┐
 浏览器 ────────▶│ NiceGUI 页面（/、/chart/{code}、/sync）   ← 纯 PG 读，永不碰 baostock │
 CLI(rich TUI) ─▶│ REST /api/sync/run|status|stop|clear-halt                          │
 cron(curl) ────▶│      └─▶ SyncRunner（唯一 worker 线程）─▶ SyncEngine                │
                 │            逐码逐数据集切片：fetch → upsert → 推水位（同一事务）       │
                 │            └─▶ BaostockProvider（限流 90/min·登录守卫·watchdog）     │
                 └──────────────────────────────┬────────────────────────────────────┘
                                                ▼
                                    PostgreSQL（127.0.0.1:5432/stockdata）
```

### baostock 红线（全部由代码强制，人不用记）

| 红线 | 机制 |
|---|---|
| 限流 90 次/分钟 | 进程内滑动窗口限流器，每次查询前阻塞配速 |
| 单进程单线程 | 唯一 worker 线程 + 模块级锁 + PG advisory lock（防多实例） |
| **两次 `bs.login()` 间隔 ≥5 分钟** | 登录时间戳持久化在 PG `baostock_session`，跨进程重启生效；不足自动 sleep 补齐 |
| IP 拉黑（10001011） | 立即熔断：写持久 halt 标志、终止 run，后续 run 拒绝启动直到人工 `clear-halt` |
| 网络接收错误（10002007） | relogin 自愈；连续 5 次才升级熔断 |
| 库内 recv 死循环 | watchdog 硬超时（600s）注入异常中断，弃用连接 |
| 僵死长连接 | 空闲 ≥15 分钟自动登出，下次查询重新登录 |

启动/重启进程本身**不**触发登录（惰性登录，首个同步任务才 login）。

## 部署与日常操作

```bash
cp .env.example .env    # 填 STOCKDATA_PG_DSN
uv run stockdata db init          # 初始化 schema（幂等）
./up.sh                           # 构建并拉起 app 容器（host 网络，:8050）
```

- Web: <http://127.0.0.1:8050>
- 同步（三选一，效果相同）：
  - Web `/sync` 页点「启动同步」；
  - `uv run stockdata sync run --tui`（终端 rich 仪表；`--plain` 输出日志行）；
  - cron：`uv run stockdata sync run --plain` 或
    `curl -X POST http://127.0.0.1:8050/api/sync/run -H 'content-type: application/json' -d '{}'`。
- 其他命令：
  ```bash
  uv run stockdata sync status       # 服务状态 + 全库水位概览 + 运行历史
  uv run stockdata sync stop         # 完成当前切片后干净停止
  uv run stockdata sync clear-halt   # 确认解封后清除熔断
  uv run stockdata sync run --codes sh.600000,sz.000001   # 指定代码
  uv run stockdata sync run --watchlist-only --datasets k_d,k_5
  uv run stockdata db reset --yes    # ⚠️ 删全部表重建（数据重新同步）
  ```

## 数据面 API（/api/v1，供下游拉取）

只读 RESTful JSON API，全部纯 PG 查询、绝不触碰 baostock。
**完整接口文档（参数/示例/约定）见 [`API.md`](API.md)**；在线 Swagger 见 `/docs`。
响应统一信封 `{"data": ..., "meta": {...}}`；错误为 `{"detail": "..."}` + 标准状态码。

| 端点 | 说明 |
|---|---|
| `GET /api/v1/kline/{code}?freq=5/30/d/w&start&end&adjust=none/fore/back&limit` | K 线（读时复权） |
| `POST /api/v1/kline/batch`（body: codes/freq/start/end/adjust/limit_per_code） | 批量 K 线（codes ≤ 500） |
| `GET /api/v1/adjust-factors/{code}` · `POST /api/v1/adjust-factors/batch` | 复权因子 |
| `GET /api/v1/securities?type&status&q&limit&offset` · `GET /api/v1/securities/{code}` | 证券列表/详情（含最新行业） |
| `GET /api/v1/trade-calendar?start&end&only_trading` | 交易日历 |
| `GET /api/v1/industries?date` | 行业分类快照（缺省最新） |
| `GET /api/v1/index-constituents/{sz50\|hs300\|zz500}?date` | 指数成分（缺省最新） |
| `GET /api/v1/financials/{code}?type&start&end` · `POST /api/v1/financials/batch` | 八类报表（含快报/预告） |
| `GET /api/v1/dividends/{code}?year` · `POST /api/v1/dividends/batch` | 分红除权 |
| `GET /api/v1/macro/{kind}?start&end` | 宏观（利率/RRR/货币供应） |
| `GET /api/v1/meta/watermarks?code&dataset&limit&offset` | **数据新鲜度**（下游先查水位再拉数） |

鉴权：`STOCKDATA_API_KEY` 为空（默认）不鉴权；配置后所有 `/api/v1/*` 要求
`X-API-Key` 请求头（`/api/sync/*` 内部控制面不受影响）。

## 同步模型

- 数据集：市场级（交易日历/证券列表/股票快照/行业/3 指数成分/5 类宏观）+
  按码（基本信息/日K/周K/复权因子/分红/6 类季报/业绩快报/业绩预告），
  分钟线（5 分、30 分）为独立第二遍（全部码的日频先跑完，日线先可用）。
- **每 (code, dataset) 一条水位**（`sync_watermark`）：覆盖区间 `[first_date, last_date]`
  即持久断点。切片 = 断点续传最小粒度：抓取 → upsert → 推水位在同一事务，
  任意时刻杀进程/断网都能从断点续跑。空结果只在「已结算边界」内推进水位
  （日线=昨天、周线=上一收盘周五、宏观=60 天前、财报=披露截止日），未结算尾部绝不虚报。
- 复权因子事件驱动：出现比因子表更新的除权除息日即全量重抓（前复权因子依赖全历史）。
- **崩溃恢复**：启动时把上一进程遗留的 `running` 孤儿 run 收尾成 `interrupted`；
  最新一条是 `interrupted`（崩溃或关停打断，非用户主动 stop/熔断 halted）则以原参数
  自动续跑，水位保证从断点继续。开关 `STOCKDATA_RESUME_INTERRUPTED_ON_START`（默认开）。
- **熔断分类与自动探测**：halt 分 `blacklist`（10001011 拉黑，只能人工 clear-halt）
  与 `login_error`（连续网络接收/登录异常升级）两类；后者由 worker 每
  `STOCKDATA_HALT_PROBE_INTERVAL_HOURS`（默认 4）小时探测一次登录，成功自动
  清除熔断并续跑被打断的任务；探测发现拉黑则升级为 blacklist 停止探测。
- **尾部修正**：日/周线每次增量把起点拉回最近 `STOCKDATA_TAIL_REFRESH_DAYS`
  （默认 5）个自然日，覆盖盘后修正（turn/pe 等衍生列）；单片单调用、不增加调用数。
- **指数**：关注列表可加指数码（如 sh.000001）——日/周线正常同步，
  分钟线阶段自动跳过（baostock 不支持指数分钟线）。
- **体检**：`uv run stockdata check`（或 `GET /api/v1/meta/gaps?code=…`）对照交易日历
  找日 K 缺口（缺口=停牌或真缺，需人工判断）；Web 所有页面顶部有全局横幅
  （熔断=红条 / 关注列表日K滞后 >2 交易日=黄条），worker 每小时也 log 滞后告警；
  Prometheus 指标见 `GET /metrics`。
- K 线只存**不复权**原始值；前/后复权在读时由 back 因子推导（`后=raw×B(t)`，
  `前=raw×B(t)/B(latest)`），存量因子永不过期。

### 全量同步耗时预期（90 次/分钟上限）

| 场景 | 调用量级 | 连续耗时 |
|---|---|---|
| 全 A ~5400 码 · 财报回填到 2020（默认） | ≈100 万次 | ≈8 天（可断点分多晚跑） |
| 全 A · 财报全历史（改 `FINANCIAL_BACKFILL_FLOOR`） | ≈280 万次 | ≈21 天 |
| 稳态增量（每日一跑） | 每码 ~2–4 次 | ≈3 小时 |
| 只同步关注列表 | 每码 ~190 次 | 每码 ~2 分钟 |

## 开发

```bash
uv sync
uv run pytest            # 单测 + 集成测试（集成用 stockdata_e2e 库，连不上自动跳过）
uv run stockdata serve   # 本机起服务（等价容器内进程）
```

- 测试全程 **FakeProvider/假 bs 模块**，不触网；实网冒烟须尊重 5 分钟登录间隔。
- 代码结构：`src/stockdata/`
  - `provider/` baostock 封装（错误分类/重试/熔断/登录守卫）
  - `sync/` 引擎（planner 切片、writers upsert、engine 编排、runner 常驻线程）
  - `web/` NiceGUI 页面与 REST API；`db/` schema 与查询；`cli.py` Typer 入口
