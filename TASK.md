# 新一轮任务：懒加载 · 命令式同步 + 读纯走 PG

> 来源：2026-06-26 架构讨论。结论 = baostock 是「批量同步源」而非「在线按需后端」，
> 把它移出读路径。读纯走 PG；抓取改为**按票为单位、懒加载、命令式、可断点续传**的同步任务。
> 状态：⬜ 待办 / 🔄 进行中 / ✅ 完成
>
> 上一轮「dotnet 独占数据」迁移清单（P1–P8，已完成）见 git 历史 `git show HEAD~N:TASK.md`。
> 本轮**反转**上一轮结尾的「读穿透按需抓取」决策（旧 TASK §5 末条）。

## 0. 头号约束 ⚠️ baostock 黑名单 / 连接易断（不变 + 本轮新事实）

**不能频繁重启 baostock 抓取进程，会触发 IP 黑名单（10001011）。重启间隔必须 > 5 分钟**
（每次重启 = 一次新 `bs.login()`）。限流 60/min、串行、退避全程保留，不得绕过。

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
   懒触发(读miss)──建 pending──▶ │  SyncStockAsync(code): 顺序 Ensure* │──▶ fetch(:8090) 60/min
                                │   basic→k_d→adjust→dividend→fin→perf│   串行、续传(水位=断点)
                                │  SyncMinuteAsync(code): 5/15/30/60   │
                                └──────────────────────────────┘
                                              │ 落盘 PG + 推水位
   MCP 工具读 ─▶ ReadService ─▶ 纯 PG SELECT ─▶ 返回（ServeFromPgOnly=true，永不碰 baostock）
                     └─ 读到未登记的 code → 异步登记 synced_stock + 建 pending task（不阻塞）
```

逐点对应：
1. **断点续传**：每票同步顺序跑各数据集，`data_watermark` 即持久断点（Coverage：Fresh 跳过），
   `stock_sync_task.datasets_done` 作粗粒度快跳。socket 断/熔断 halt 后重跑自动从断点续。配速 60/min。
2. **不全量**：取消全市场遍历，只同步**被查询过**的票。
3. **任务接口 = 单票**：提交 `code` → 按任务状态把该票**所有数据集**同步完。
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
    `StockData:Sync:StaleAfterHours`(默认 20)、`StockData:Sync:RatePerMinute`(默认 60)。
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
  - **实跑验证（mcp 重建部署，未碰 fetch）**：`/sync/status`→空表结构；`/sync/market`→`{done, snap_date:2026-06-26}`；
    `/sync/stock?code=sh.600000`→ registered=1、status 流转、`datasets_done` 实时推进 `{stock_basic→k_d→adjust_factor→
    dividend→…}`、真数据落盘(dividend 22/financial 12↑/adjust 26)、fetch 全程 healthy 无 halt（① 自愈生效）。
    `dotnet test` 162/162。
  - ⚠️ **冷启全史单票同步很慢**：financial 每季在 worker 跑 6 类 baostock 查询，~110 季×6≈660 次/60min ≈ 十几分钟；
    属懒加载一次性成本，**可续传**（Coverage 跳已抓季）。后续可加“近 N 年”上限配置收敛（见 R2）。
  - ⬜ **未做：接外部 cron + 翻 `ServeFromPgOnly=true`**——待确认夜间 /sync/run 能持续喂数后再切（把 baostock 移出读路径）。
- ⬜ **P3 分钟线特殊任务**
  - `SyncMinuteService.SyncMinuteAsync(code)`：k_5/15/30/60 全历史（复用 RangeSlicer 730 切片，每切片水位=续传点）。
    `kind='minute'`；命令 `POST /sync/stock?code=&minute=true`（或独立端点）。默认全量同步不含分钟线。

## 4. 风险与失效模式

- **R1 断点不幂等** → 续传重复抓 / 漏数据。缓解：依赖已绿的 Coverage（Fresh 跳过）+ datasets_done 双层；续传对照测试。
- **R2 单票同步耗时长撞限流超时**：60/min + 多数据集，单票可能数十次查询。缓解：编排**串行**逐查询提交
  （限流等待 ~1s/次 < FetchWaitTimeout 120s）；必要时调大 dotnet 侧 `FetchWaitTimeout`。
- **R3 长批次触发熔断 halt**：靠 §0 自愈 + 熔断阈值；halt 后任务标 partial，`/sync/run` 下轮续，不丢进度。
- **R4 市场级数据缺失致日期运算错**：cron 必须先 `/sync/market` 保 trade_calendar 新鲜；懒加载不覆盖市场级。
- **R5 首读返回空**：懒加载下，新票首查 PG 空。约定：读返回空/部分 + 已入队，下轮同步后补齐（可接受，非阻塞）。

## 5. 明确不做 / 暂不动

- 不做全市场全量遍历同步（只同步查询过的，懒加载）。
- 不做 realtime / 盘中实时刷新（EOD 模型；分钟线也是显式全量任务，非实时）。
- 不引入 dotnet 常驻调度器（命令式，调度交外部 cron）。
- 不放松限流 / 不引入并发多会话抓 baostock（§0）。
