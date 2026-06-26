# 新一轮任务：懒加载 · 命令式同步 + 读纯走 PG

> 来源：2026-06-26 架构讨论。结论 = baostock 是「批量同步源」而非「在线按需后端」，
> 把它移出读路径。读纯走 PG；抓取改为**按票为单位、懒加载、命令式、可断点续传**的同步任务。
> 状态：⬜ 待办 / 🔄 进行中 / ✅ 完成
>
> 上一轮「dotnet 独占数据」迁移清单（P1–P8，已完成）见 git 历史 `git show HEAD~N:TASK.md`。
> 本轮**反转**上一轮结尾的「读穿透按需抓取」决策（旧 TASK §5 末条）。

## 0. 头号约束 ⚠️ baostock 黑名单 / 连接易断（不变 + 本轮新事实）

**不能频繁重启 baostock 抓取进程，会触发 IP 黑名单（10001011）。重启间隔必须 > 5 分钟**
（每次重启 = 一次新 `bs.login()`）。限流 90/min、串行、退避全程保留，不得绕过。

本轮新增已上线的连接韧性（2026-06-26，见 `baostock.py`）：
- **`10002007 网络接收错误`已改判为可自愈**：长连接被服务端断开后 recv 空包所致，relogin 即恢复。
  连续 < `STOCKDATA_FETCH_RECEIVE_ERROR_HALT_THRESHOLD`(默认5) 次 → 重登录重试；达阈值才熔断暂停。
  **真拉黑只认 10001011。**
- **空闲自动登出**：worker 闲置 ≥ `STOCKDATA_FETCH_IDLE_LOGOUT_SECONDS`(默认900s/15min) 主动登出关 socket。
- → 本轮“长批次单票同步”因此可行：偶发断连自愈，不再每几分钟人工 /restart。

## 1. 目标形态（已定方向，对应讨论 6 点）

```
   外部 cron ──POST /sync/run──▶ ┌──────────────────────────────┐
   外部 cron ──POST /sync/market▶│ dotnet 同步编排（命令式，无常驻调度）│── 唯一调 fetch 处
   懒触发(读miss)──建 pending──▶ │  SyncStockAsync(code): 顺序 Ensure* │──▶ fetch(:8090) 90/min
                                │   basic→k_d→adjust→dividend→fin→perf│   串行、续传(水位=断点)
                                │  SyncMinuteAsync(code): 5/15/30/60   │
                                └──────────────────────────────┘
                                              │ 落盘 PG + 推水位
   MCP 工具读 ─▶ ReadService ─▶ 纯 PG SELECT ─▶ 返回（ServeFromPgOnly=true，永不碰 baostock）
                     └─ 读到未登记的 code → 异步登记 synced_stock + 建 pending task（不阻塞）
```

逐点对应：
1. **断点续传**：每票同步顺序跑各数据集，`data_watermark` 即持久断点（Coverage：Fresh 跳过），
   `stock_sync_task.datasets_done` 作粗粒度快跳。socket 断/熔断 halt 后重跑自动从断点续。配速 90/min。
2. **不全量**：取消全市场遍历，只同步**被查询过**的票。
3. **任务接口 = 单票入队**：`POST /sync/stock?code=` **只入队**（标 pending）立即返回，**不内联抓**；
   真正抓取由唯一串行消费者 `/sync/run` 处理。**baostock 单连接不许并发** → 所有入口只入队、单消费者排他抓
   （2026-06-26 修正：原 /sync/stock 前台阻塞同步会与 /sync/run 并发驱动 baostock，已改）。
4. **无 realtime**：分钟线为**独立任务** `SyncMinuteAsync`，显式下达才同步 5/15/30/60 全历史。
5. **懒加载 + 注册表**：`synced_stock` 记录纳管的票；读到未知票即登记。
6. **命令式**：dotnet 暴露 `/sync/*` 命令，自身不常驻调度；何时跑由**外部 cron** 决定。

## 2. 待拍板决策（已定）

- ✅ **D-1 任务态存哪【PG 表】**：`stock_sync_task` / `synced_stock` 落 PG（属主一致、持久、可续传），
  不用 Redis。fetch_service 的 Redis job 仍是底层单次查询去重，层级不同。
- ✅ **D-2 市场级数据【独立命令 `/sync/market`】**：`trade_calendar`(日期运算刚需)/`stock_list`/`industry`/
  `index_constituent` 不属任何单票，由 cron 每日先调 `/sync/market` 各同步一次（单次调用、便宜）。不逐票。
- ✅ **D-3 懒登记触发点【读路径内，异步不阻塞】**：读到未登记 code → 顺手 upsert `synced_stock` +
  建 pending `stock_sync_task`，立即返回 PG 现有内容（首读可能为空）。
- ✅ **D-4 稳态再同步【`/sync/run` 内部按过期重抓】**：`/sync/run` 扫 pending + `last_synced` 过期者续传，
  无需 cron 显式重置状态。过期阈值配置化（`StockData:Sync:StaleAfterHours`，默认 20h）。

## 3. 阶段（增量、全程开关、可回退）

- ✅ **P0 schema 地基（2026-06-26）**
  - ✅ EF 实体 `SyncedStock`(synced_stock) + `StockSyncTask`(stock_sync_task，PK=code+kind，
    datasets_done text[]) + DbSet；迁移 `20260626020447_AddSyncTables`（纯新增 2 表，零 Alter）。
    **已 apply 到运行库 stockdata**（CreateTable only，无 fetch 重启，§0 无关）；两表结构经 psql 核对。
    `dotnet test` 162/162。
  - ⬜ 配置项（默认安全，P1/P2 用到时在读取处带默认值即可，无需写 appsettings）：
    `StockData:ServeFromPgOnly`(默认 false)、`StockData:Sync:Enabled`(默认 false)、
    `StockData:Sync:StaleAfterHours`(默认 20)。限流在 fetch 侧（`FETCH_RATE_LIMIT_PER_MINUTE`，现 90/min）。
- ✅ **P1 读路径纯 PG + 懒登记（2026-06-26）**
  - 10 个 `*ReadService` 全部加 `ServeFromPgOnly` 门控（`config.GetValue<bool>("StockData:ServeFromPgOnly")`，
    默认 false）：true 时跳过 `EnsureXxxAsync` → 纯 PG 读。改动复用各方法已开的 scope/db，**未动构造函数**。
    - 按票（登记 + 跳穿透）：Kline/KlineMinute/StockBasic/Dividend/Financial(3法)/AdjustFactor。
    - 市场级（仅跳穿透，不登记）：Macro/TradeCalendar/TradingDays/Snapshot(三件套+Resolve/PreviousTradingDay 静态助手透传 pgOnly)。
  - `SyncRegistry.RegisterIfNewAsync(db, code)`：单条 CTE 幂等 upsert `synced_stock` + pending `stock_sync_task(full)`，
    一次往返、不触 baostock。**PG 实跑验证**：首次插 1、二次幂等(INSERT 0 0)、最终各 1 行。
  - 旧穿透为安全网：开关默认 false → **现网零行为变化**；翻 true 才进“纯 PG + 懒登记”态（待 P2 命令就绪后切）。
  - `dotnet build` 0 error、`dotnet test` 162/162。**未部署**（开关默认 false，部署与否当前等价；随 P2 一起上）。
- ✅ **P2 单票同步编排 + 命令接口（核心，2026-06-26，已部署+实跑验证）**
  - `Data/SyncServices.cs`：3 个单例（各自建 scope，复用现成 Ensure）：
    - `StockSyncService.SyncStockAsync(code)`：顺序 `EnsureXxx`（stock_basic→k_d→adjust→dividend→financial→
      performance），每步幂等续传；写 `stock_sync_task`(pending→running→done/partial/failed)，**每步完成即落 datasets_done**
      （步级断点）；dividend/financial 下限取 ipo 年（无则 A 股 epoch）。抓取失败(FetchTimeout/FetchFailed，多为
      halt)→ 标 partial 保进度退出，**不在 halt 期硬刚**（§0）；其余异常 → failed。
    - `SyncMarketService.SyncMarketAsync()`：日历(去年初~今年末)→ 解析最近交易日 → stock_list/industry/index 三快照。
    - `SyncRunService.RunAsync(max)`：扫 pending/partial/过期(StaleAfterHours,默认20h)票逐个续传，**遇 partial 即停**下轮再续；
      `StatusAsync()` 按状态计数 + registered。
  - 端点（Program.cs，管线关时 503）：`POST /sync/stock?code=[&minute=]` / `POST /sync/run?max=` /
    `POST /sync/market` / `GET /sync/status`。DI 注册在 `AddStockDataPipeline`。
  - **队列模型（2026-06-26 修正，baostock 不许并发）**：`/sync/stock` 改为**入队即返回**（`EnqueueAsync`，
    upsert pending，不碰 baostock）；`/sync/run` 是**唯一串行消费者**，加进程内**单飞信号量**拒绝并发 run。
    `SyncStockAsync`/`SyncMinuteAsync` 仅由消费者在锁内调用。修 latent bug：续传仅 `partial` 保留 datasets_done，
    pending/过期 done 开新一轮清空（否则 datasets_done 满会假装完成不刷新）。
  - **实跑验证（mcp 重建部署，未碰 fetch）**：`/sync/status`→空表结构；`/sync/market`→`{done, snap_date:2026-06-26}`；
    `/sync/stock?code=sh.600000`→ registered=1、status 流转、`datasets_done` 实时推进 `{stock_basic→k_d→adjust_factor→
    dividend→…}`、真数据落盘(dividend 22/financial 12↑/adjust 26)、fetch 全程 healthy 无 halt（① 自愈生效）。
    `dotnet test` 162/162。
  - ⚠️ **冷启全史单票同步很慢**：financial 每季在 worker 跑 6 类 baostock 查询，~110 季×6≈660 次 ÷ 90/min ≈ 7+ 分钟；
    属懒加载一次性成本，**可续传**（Coverage 跳已抓季）。后续可加“近 N 年”上限配置收敛（见 R2）。
  - ⬜ **未做：接外部 cron + 翻 `ServeFromPgOnly=true`**——待确认夜间 /sync/run 能持续喂数后再切（把 baostock 移出读路径）。
- ✅ **P3 分钟线特殊任务（2026-06-26，代码完成，未部署）**
  - 重构 `StockSyncService`：抽出共用任务骨架 `RunTaskAsync(code, kind, work)`（登记→加载/新建 task→running→逐步
    幂等+落 datasets_done→done/partial/failed），full 与 minute 共用，零重复。
  - `SyncMinuteAsync(code)`（kind=minute）：`SyncRegistry.EnableMinuteAsync` 置 minute_enabled，逐 freq
    k_5/15/30/60 调 `KlineMinuteService.EnsureRangeAsync`（floor=MinuteBackfillStart 2023-01-01，RangeSlicer 730 切片，
    每切片水位=续传点）；datasets_done 记 `k_5/k_15/k_30/k_60`。
  - 端点 `POST /sync/stock?code=&minute=true` → SyncMinuteAsync（去掉 501）。
  - `/sync/run` 升级：不再限 kind=full，**full 与 minute 都续传**（按 kind 分派 SyncStockAsync/SyncMinuteAsync），
    minute 仅对已 enable 的票存在 → 天然 opt-in。
  - `dotnet build` 0 error、`dotnet test` 162/162。**未部署**（按 David 要求；分钟路径复用 P2 已实跑的 RunTaskAsync
    骨架 + 已部署的 KlineMinuteService，置信高）。
  - 观测（P2 部署期跑的 sh.600000 全量同步）：client 600s 超时断开后**服务端编排继续推进**（financial 已 348 行
    = 6 类×58 季），证明单票同步对断连健壮。**结论**：cron 应放长超时或 fire-and-forget + 轮询 /sync/status，
    勿用短超时同步阻塞。

## R1. 架构纠正：队列 + 常驻 Drainer + fetch 优先级（2026-06-26，代码完成未部署）

讨论纠正三点误区：① cron 只触发、不等返回；② cron 生成队列、交后台处理；③ 无单票 URL，
同步由 MCP 读触发（必须时先触发同步、再输出）。落地为**方案 A（有界等待）+ 常驻 Drainer +
不引入 Redis（保留 HTTP poll）+ fetch 高/低优先级**。

- ✅ **fetch 优先级（Python，server/fetch_service）**：`jobs.py` 加 `fetch:pending:high` 队列，
  `submit(...,priority)` 按优先级 lpush，`next_pending` BRPOP `[high, low]` 优先序；`app.py` `/fetch`
  body 收 `priority`（默认 low）。**仅代码、未重建 fetch**（重建=新 bs.login，§0，待 David 定时机）。
- ✅ **dotnet fetch 优先级**：`FetchPriority`（AsyncLocal，免穿透改 Ensure 签名）；`HttpFetchClient`
  POST 带 `priority = IsHigh?high:low`；`WaitTimeoutSeconds` 默认 120→**600**（对齐 watchdog，修慢查询误判超时）。
- ✅ **常驻消费者 `SyncDrainer`(BackgroundService)**：唯一串行驱动 baostock 的后台 worker——循环：
  查 halt → **按间隔自维护市场级（无论队列空否，日期运算前置）** → 取 pending/partial 任务 →
  SyncStockAsync/SyncMinuteAsync（low 优先）→ 空队列 sleep；**halt 时暂停提交**等 FetchHaltMonitor 恢复；
  partial 退避。取代旧的阻塞式 /sync/run + 信号量。注册在 `AddStockDataPipeline`（AddHostedService）。
- ✅ **MCP 读路径（方案 A）**：`ReadFetch.EnsureAsync(config, pgOnly, ct, ensure)` 统一收口——pgOnly 时
  `FetchPriority.High` + `ReadFetchBudgetSeconds`(默认30s) 有界等待，超预算/失败**吞异常回退 PG**（后台 Drainer 续抓），
  真实请求取消不吞。10 个 ReadService 全改为：pgOnly 时登记(按票) + ReadFetch 定向高优先抓；非 pgOnly 维持旧穿透。
- ✅ **控制面瘦身**：删 `/sync/stock`(单票 URL)、阻塞 `/sync/run`、`/sync/market`（市场级改由 Drainer 自维护）；
  `SyncRunService` 改为 `RefreshAsync`（cron「生成队列」：把过期 done 重置 pending，一条 UPDATE 秒回）+ `StatusAsync`。
  **对外端点只剩 `POST /sync/refresh`、`GET /sync/status`**（+ `/mcp`、`/healthz`）。
- 修 latent bug：续传仅 `partial` 保留 datasets_done；pending/过期 done 开新一轮清空（否则假装完成不刷新）。
- `dotnet build` 0 error、`dotnet test` 162/162。**未部署**（dotnet 重建不碰 baostock；但 fetch 重建=新 bs.login，
  两者一起部署时机交 David，§0）。

## 6. 收尾（待 David 拍板/部署）

- ⬜ 部署 R1：重建 **fetch**（拿优先级队列，=新 bs.login，§0 间隔 >5min）+ **mcp**（Drainer/方案 A 读路径）。
- ⬜ 接外部 cron：收盘后 `POST /sync/refresh`（秒回，生成队列）；市场级由 Drainer 自动刷或手动 `POST /sync/market`。
  **cron 只触发不等待**；抓取全在常驻 Drainer。
- ⬜ 验证 Drainer 能持续喂数后，翻 `StockData:ServeFromPgOnly=true`，启用方案 A 读路径（懒登记 + 定向高优先有界抓 + PG 回退）。
- ⬜ 可选优化：dividend/financial 加“近 N 年”上限配置，收敛冷启全史成本（见 R2）。

## 4. 风险与失效模式

- **R1 断点不幂等** → 续传重复抓 / 漏数据。缓解：依赖已绿的 Coverage（Fresh 跳过）+ datasets_done 双层；续传对照测试。
- **R2 单票同步耗时长撞限流超时**：90/min + 多数据集，单票可能数十次查询。缓解：编排**串行**逐查询提交
  （限流等待 ~1s/次 < FetchWaitTimeout 120s）；必要时调大 dotnet 侧 `FetchWaitTimeout`。
- **R3 长批次触发熔断 halt**：靠 §0 自愈 + 熔断阈值；halt 后任务标 partial，`/sync/run` 下轮续，不丢进度。
- **R4 市场级数据缺失致日期运算错**：cron 必须先 `/sync/market` 保 trade_calendar 新鲜；懒加载不覆盖市场级。
- **R5 首读返回空**：懒加载下，新票首查 PG 空。约定：读返回空/部分 + 已入队，下轮同步后补齐（可接受，非阻塞）。

## 5. 明确不做 / 暂不动

- 不做全市场全量遍历同步（只同步查询过的，懒加载）。
- 不做 realtime / 盘中实时刷新（EOD 模型；分钟线也是显式全量任务，非实时）。
- 不引入 dotnet 常驻调度器（命令式，调度交外部 cron）。
- 不放松限流 / 不引入并发多会话抓 baostock（§0）。
