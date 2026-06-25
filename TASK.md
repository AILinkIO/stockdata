# 架构迁移任务：Python 抓取微服务 + dotnet 数据属主

> 来源：2026-06-25 架构讨论。目标 = 重新划分两层职责，让 dotnet 成为唯一数据属主，
> Python 退化为无状态（对 PG 无依赖）的 baostock 抓取微服务。
> 状态：⬜ 待办 / 🔄 进行中 / ✅ 完成
>
> 上一轮 fetcher 优化清单（T1–T8，已全部完成）见 git 历史 `git show HEAD~N:TASK.md`。

## 0. 头号约束 ⚠️ baostock 黑名单

**不能频繁重启 baostock 的抓取进程，会触发 IP 黑名单。**

代码现状（`fetcher/providers/baostock.py`）：模块级单例 TCP 连接、惰性 `bs.login()`、
长连接复用到出错才 `force_relogin()`，**不做闲置预防性重登录**。即：

- **进程重启 = 一次新 `bs.login()`**。频繁重启 = 频繁登录 = 黑名单风险（参见
  记忆 baostock-blacklist-vs-code-bug：错误码 10001011）。
- 迁移期间最危险的反模式：**新旧两套抓取进程同时各自登录、或反复重建容器试错**。

### 迁移必须遵守的红线

1. **抓取服务长生命周期**：Python 抓取微服务保持单一长驻进程 + 单例 baostock 会话；
   部署用滚动/优雅重启，避免 crash-loop。健康检查失败不要无脑频繁重启。
   **重启间隔必须 > 5 分钟**（每次重启 = 一次新 `bs.login()`；间隔太短会被判异常登录频次
   → 黑名单。与 k_d 刷新周期 300s 同量级，重启不应比它更密）。健康检查重启策略、
   滚动发布、crash-loop 退避都要满足这条下限。
2. **限流不放松**：每分钟查询次数上限（redis db4，已配）+ 串行 + 退避，整套保留在
   Python 侧并继续生效，迁移全程不得绕过。
3. **同一时刻只有一个登录态**：迁移期新旧路径并存时，**只允许一套进程持有 baostock
   会话**。切流用流量切换，不要让两套都在登录抓取。
4. **本地联调省着抓**：开发/对照测试优先用已落库的历史数据做黄金对照，不要为调试反复打
   baostock。需要真打时走限流，单元测试 mock provider。

## 1. 目标形态（已定方向）

```
┌──────────────┐   HTTP (REST 契约)     ┌───────────────────────────┐
│   Python      │  POST /fetch  ───────► │  dotnet                    │
│ 抓取微服务      │  GET  /fetch/{id} ◄──► │  数据服务 + 唯一 PG 属主      │
│ 长驻 + 单例会话 │                        │                            │
│ · 限流(db4)    │   不碰 PG               │ · coverage 校验(移植)       │
│ · 退避重试      │                        │ · writer 解析+落盘(移植)    │
│ · baostock 调用 │                        │ · 读穿透编排 + 等待          │
│ · job 状态(Redis)│                       │ · beat 定时(移植)           │
└──────────────┘                        │ · MCP 工具直读 PG           │
        Redis(job+限流) ── Python 私有     │ · TA-Lib 指标(已在)         │
                          PG ◄── 仅 dotnet │                            │
                                          └───────────────────────────┘
```

- **Python**：HTTP 抓取微服务。`POST /fetch` 立即返回 `202 {job_id}`（**异步**，
  因首次回填 1990→今可达数分钟，禁止同步长连接）；`GET /fetch/{job_id}` 查状态/取
  payload。job 状态 + 去重 + 结果存 **Redis**（不再依赖 PG）。**不碰 PG**。
- **dotnet**：唯一 PG 属主。coverage 判定 → 调 Python HTTP 抓缺口区间 → 拿 payload →
  解析+落盘（数据 + 水位同事务）→ 直读 PG 服务。
- **Redis 角色**：从"跨语言总线"降为 Python 私有的 job 存储 + 限流状态。dotnet 可不连
  Redis。

## 2. 待拍板决策（开工前定）

- ✅ **D-A 抓取 API 形态【已定：异步 submit+poll】**：`POST /fetch {type, params}` 立即返回
  202 + `{job_id, status, dedup}`（禁止同步阻塞，因首次回填可达数分钟）；`GET /fetch/{job_id}`
  轮询 status/payload。dotnet 侧轮询带超时（沿用 `fetch_wait_timeout` 语义）→ 超时返回 504
  等价 → 现有 resilience 重试。**Python 只抓给定区间，不做 coverage / 不切片**（切片在 dotnet）。
- ✅ **D-B dotnet PG 访问层【已定：EF Core + LINQ】**：EF Core 为主——coverage 判定查询、
  MCP 服务读、迁移内建都走它；LINQ 强类型护住 coverage 那套日期边界逻辑（压 R1）。
  **批量 upsert 热路径用 `ExecuteSqlRaw` 发原生 `INSERT … ON CONFLICT DO UPDATE`**，
  与水位更新同 `DbContext`/事务。理由：EF Core `SaveChanges` 逐行、原生不支持 ON CONFLICT，
  批量写本就该手写 SQL。原则 = ORM 管该 ORM 的（查询/读/迁移），手写 SQL 管批量写。
- ✅ **D-C job 结果 TTL【已定，对齐现有常量】**：
  - 完成态 job + payload + dedup 索引 TTL = **600s**（沿用旧 Celery `result_expires=600`；
    dotnet 秒级轮询落盘，足够覆盖崩溃+重启+重试窗口）。
  - 在途 job 安全 TTL / 僵尸阈值 = **1200s**（沿用旧 `_STALE_RUNNING`；超时视为 worker
    异常，释放 dedup 让下次重建 job）。
  - 600s 内同 params 重请求复用缓存 payload；过后重抓（幂等，可接受）。
- ✅ **D-D schema/迁移属主【已定：EF Core Migrations + 重建库】**：schema 属主 = EF Core
  Migrations，不再引独立迁移工具。**可直接摧毁旧库重建**：EF Core 全新建表，无 in-place
  迁移、无双属主交接（R5 消除）。代价 = 旧缓存数据丢失，由读穿透/beat 按需从 baostock
  重新回填——**一次性成本，必须走限流（§0），切忌反复 nuke+refetch**（见 R5）。

## 3. 迁移阶段（增量、可回退，不大爆炸）

- ⬜ **P1 Python 抓取微服务契约固化**
  把 `providers/baostock.py` + 限流包成 `POST /fetch` / `GET /fetch/{id}`，job 状态入
  Redis。**保持单例长会话**（红线 #1）。此阶段 Python 仍可与旧 PG 路径并存（但遵守红线
  #3，只一套登录态）。产出：REST 契约文档 + Redis key/状态机/TTL 设计。
- 🔄 **P2 dotnet 落盘地基**
  - ✅ **schema 地基**：EF Core 实体 `Kline`/`DataWatermark`（`StockData.Mcp/Data/Entities/`）
    + `StockDataDbContext`（显式列映射，逐列对齐旧 alembic DDL：snake_case、numeric 精度、
    character(1)、timestamptz、PK、updated_at default now()、code default ''）+ 设计期工厂
    `StockDataDbContextFactory`（复用 `STOCKDATA_PG_DSN`，含 URL→Npgsql 解析，4 个解析测试绿）。
    迁移 `Data/Migrations/*_InitialSchema` 已生成，Up() DDL 经核对与旧库逐列一致。
    包：`Npgsql.EntityFrameworkCore.PostgreSQL` + `Microsoft.EntityFrameworkCore.Design`（10.0.0）；
    全局工具 `dotnet-ef` 10.0.0。`dotnet test` 74/74。
    注：**迁移尚未 apply 到运行中的库**——`database update` = 摧毁重建动作，留到 P8 切换
    （现库仍被运行中的 Python 栈占用，同名表会冲突）。
  - ✅ **writer 移植**（随 P4）：`KlineParser`（_dec/_int/_date/_bool01 + _K_COL_MAP，
    **字符串直转 decimal 不经 float**，空串→null，8 个单测绿）+ `KlineWriter`（`ExecuteSqlRaw`
    批量 `ON CONFLICT DO UPDATE` 1000 行/批 + 水位 GREATEST/LEAST upsert，**同 DbContext 事务**）。
- ✅ **P3 dotnet coverage 移植（最高风险）—— 纯逻辑层完成**
  `coverage.py` 整体移植到 `StockData.Mcp/Coverage/Coverage.cs`（命名空间 `StockData.Mcp.Data`）：
  `CheckRange`/`CheckQuarter`/`CheckSnapshot`/`ClaimableLast`/`SettledBoundary`/`QuarterEnd`/
  `QuarterDisclosureDeadline`/`MergeRanges` 全移。`Watermark` 入参 record 与 EF 实体解耦。
  **黄金对照 `CoverageTests.cs` 38 个 case（方法名 1:1 对齐 `test_coverage.py`）全绿**，
  含三段缺口、未定型尾部节流、上次抓取定型边界回退、周/月/宏观沉淀、财报披露截止、
  快照、claimable_last、未来钳制。`dotnet test` 70/70 通过无回归。
  注：`test_readthrough_slicing` 的切片对照留到 P4（切片在 dotnet `EnsureRange`，非 coverage）。
- 🔄 **P4 日线 k_d 端到端打通 —— 代码+单测完成，live E2E 待跑**
  - ✅ **dotnet 编排骨架**：`KlineService.EnsureRange`（coverage → 切片 → 抓取 → 落盘）
    + `RangeSlicer`（切片，6 个对照 `test_readthrough_slicing` 全绿）+ `IFetchClient`/`HttpFetchClient`
    （submit+poll，超时→504 等价）+ `EfWatermarkStore`。编排用 fake 三件套单测（命中新鲜/首次全史切片/
    尾部缺口）。
  - ✅ **Python `/fetch` 微服务**：`fetch_service/`（`app.py` POST/GET、`jobs.py` Redis job
    去重/状态/TTL、`worker.py` 串行消费复用 `providers.baostock` 限流+登录+退避）。默认 job 存 redis db5。
    import 冒烟通过。
  - ✅ **契约对齐**：Python snake_case JSON ↔ dotnet DTO（`HttpFetchClient.Json` snake_case 策略），
    4 个往返测试绿。`dotnet test` 95/95。
  - ✅ **DI 接线（不碰现网）**：`ServiceCollectionExtensions.AddStockDataPipeline`（注册 DbContext/
    `IWatermarkStore`/`IKlineWriter`/`IFetchClient` typed HttpClient/`KlineService`/TimeProvider）。
    Program.cs **默认关闭**，仅 `StockData:PipelineEnabled=true` 时注册 → 现 MCP 工具仍走旧 REST，
    运行栈零改动。MCP 工具尚未路由到 KlineService（= 切换动作，留 P8）。
  - ✅ **隔离空库 E2E 脚手架**：`KlinePipelineE2ETests`（`[Trait Category=E2E]`，真 PG 独立空库 +
    **fake fetch 不打 baostock**）验证 coverage→切片→落盘(EF/ON CONFLICT 同事务)→直读→重判新鲜。
    护栏拒绝库名 stockdata；默认跳过（需 `STOCKDATA_E2E_PG_DSN`）。脚本 `scripts/e2e-kline.sh`
    自动从 .env 推导独立库 `stockdata_e2e` 并预建。**已实跑通过**：2 行落库、close=10.5000 精确、
    水位 first=2020-01-01(LEAST)/last=2024-01-03(claimable)、重判 fresh 不再 fetch。普通套件 96/96。
  - ⬜ **live baostock E2E（你来跑，谨慎）**：把 MCP `get_historical_k_data` 路由到 `KlineService`；
    启 `fetch_service`（db5）；隔离库换真 fetch（`STOCKDATA_E2E_FETCH_URL`/启用管线指向 fetch_service）；
    新 code 回填验证。**走限流、进程不频繁重启、重启间隔 >5min**（§0）。
- ⬜ **P5 扩展其余数据类型**
  `check_quarter`（财报披露截止日 + 负结果记忆）、`check_snapshot`（快照永久有效）、
  K线其余频率、复权因子、宏观。每类沿用黄金对照。
- ⬜ **P6 beat 移植到 dotnet** ⚠️ landmine
  beat 现靠读 PG 决定调度（`_is_trading_day` 查日历、`sync_tracked_codes` 遍历水位），
  PG 只在 dotnet 后**只能在 dotnet 跑**（Quartz.NET / Hangfire / BackgroundService+cron）。
  4 个任务（日历 08:00 / 昨日列表 08:30 / 市场 17:00 / 已入库代码 17:10 + 交易日 gating）
  整体移植，读 PG 决定 → 调 `POST /fetch`（复用去重）。
- ⬜ **P7 Python 瘦身**
  删 `api/`、`db/`(session/models/coverage)、`writer.py`、`beat.py`、Celery 与
  SQLAlchemy/psycopg/alembic 依赖；只留抓取 + 限流 + Redis job + HTTP。镜像变小。
- ⬜ **P8 切换与验证**
  流量切到 dotnet 路径（红线 #3：切换瞬间只一套登录态）；验证全类型读穿透、定时同步、
  幂等重投；保留回退开关。

## 4. 风险与失效模式

- **R1 coverage 移植不等价**（最高）→ 永久空洞 / 反复空抓拉黑。缓解：黄金对照测试。
- **R2 writer 解析失真**（Decimal/date/空串）→ 数据错。缓解：逐字段对照现有落库结果。
- **R3 抓取进程 crash-loop**（红线 #1）→ 重登录拉黑。缓解：优雅重启、健康检查不触发频繁
  重启、单例长会话。
- **R4 新失效模式**：Python 抓成功、dotnet 落盘前崩 → 靠幂等 upsert + dotnet 重 POST
  （Python 在 TTL 内返回缓存 job）兜住。
- **R5 重建库后的全量重回填**：摧毁旧库 → 跟踪标的首次触达触发 1990→今全史回填，量大。
  缓解：必走限流（§0 红线 #2）、安排在非交易时段一次性预热、避免反复重建。
  注：§0 红线是"进程不频繁重启/重登录"，与"一次性大批量查询"不冲突——限流管住量即可。

## 5. 明确不做 / 暂不动

- 旧数据不做迁移保留：可摧毁旧库由 EF Core 重建，数据靠读穿透/beat 重新回填（D-D）。
- 不放松限流 / 不引入并发多会话抓 baostock（红线 #2/#3）。
- provider 抽象不扩（当前仅 baostock 一个实现，interface 很轻，不算过度设计）。
