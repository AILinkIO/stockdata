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
  - ✅ **live baostock E2E（已实跑通过，2026-06-25）**：正式切换主库 `stockdata`——
    `DROP SCHEMA public` + EF `database update` 建 kline/data_watermark；compose 加 `fetch` 服务
    （server 镜像跑 `uvicorn fetch_service.app`，:8090）+ mcp 开 `StockData__PipelineEnabled=true`
    + PG DSN + FetchBase，`./up.sh fetch mcp`（不启旧 api/migrate）。`LiveKlinePullTests` 驱动
    `KlineService.EnsureRange(sh.600000,k_d,...)` 首次触达全史回填：4 段切片 → fetch_service → baostock
    →落盘。**结果**：6446 行(1999-11-10 实际上市日→2026-06-18)、水位 first=1990-12-19/last=2026-06-20、
    decimal 精确、单次登录无重登风暴、两容器长驻 healthy。§0 达标。
    遗留：MCP 工具的真正协议调用（serving 读路径）未走 MCP JSON-RPC 验证（用同一 KlineService 代码 +
    序列化已单测，置信高）；非 k_d 工具因未启 api 失效（预期）。
- 🔄 **P5 扩展其余数据类型**
  - ✅ **schema 地基（8 表，2026-06-25）**：EF 实体 `AdjustFactor`/`Dividend`/`FinancialReport`/
    `StockBasic`/`TradeCalendar`/`StockListSnapshot`/`IndexConstituent`/`StockIndustry`（覆盖 范围/
    快照/季度财报/分红 四模式）+ snake_case 约定（`ToSnakeCase` 循环，避免逐列手写；对既有
    kline/data_watermark 幂等）+ updated_at default now() 约定。迁移 `AddRemainingTables` 仅新增 8 表
    （零 Alter，不碰既有表），**已 apply 到运行中的 stockdata**（kline 6446 行完好）。`dotnet test` 108/108。
    coverage 判定（CheckQuarter/CheckSnapshot/CheckRange）P3 已移植且测试绿。
  - 🔄 **数据管线（writer/parser/orchestration/读取/路由）**：按 k_d 样板逐类做。
    - ✅ **trade_calendar 完整管线（代码+单测，2026-06-25）**：`FetchRequest` 泛化为 `{Type, 可选 Code/Frequency}`
      支持无 code 类型；`TradeCalendarParser`（calendar_date+is_trading_day）+ `TradeCalendarWriter`
      （ON CONFLICT + **水位 last=区间末，不用 claimable_last**，code=""）+ `TradeCalendarService`（coverage→
      不切片→抓取→落盘）+ `TradeCalendarReadService`（EnsureRange+直读+序列化 [{calendar_date,is_trading_day}]）；
      `get_trade_dates` 工具走开关路由；fetch_service worker 加 `fetch_trade_calendar`。`dotnet test` 113/113。
      **未部署**（需重建 fetch 镜像=重启=新 bs.login，按 §0 待 David 在 >5min 间隔时决定）。
    - ✅ **快照四件套数据管线（代码+单测，2026-06-25）**：`SnapshotService`（CheckSnapshot 点状编排）+
      4 个 ingest（`StockBasicIngest` per-code+today / `StockListIngest` 空结果不写水位 / `IndustryIngest` /
      `IndexConstituentIngest` data_type=index_{code}）+ 通用 `SnapshotSql`（分块 ON CONFLICT upsert + 水位 +
      字段访问器）。`FetchRequest` 再泛化为命名可选字段 + `ToParams()`（支持 snap_date/index_code）。
      fetch_service worker 加 4 类分发。**stock_basic 读+路由**（get_stock_basic_info，无日历依赖，单对象序列化，
      中文 UTF-8 原样输出对齐 Starlette）。`dotnet test` 119/119。
      遗留：stock_list/index/industry 的**读+路由**需 snap_date="最近交易日"解析（依赖 trade_calendar 数据 +
      DateTools 派生逻辑迁移）；派生工具（search_stocks/get_suspensions/list_industries/get_industry_members）；
      **未部署**（需重建 fetch=重启，§0）。
    - ✅ **adjust_factor + 复权解锁（代码+单测，2026-06-25）**：`AdjustFactorParser` + `AdjustFactorWriter`
      （整段 upsert + 水位，空结果也推进）+ `AdjustFactorService.EnsureFull`（**恒从 1990 整段抓取**，
      coverage 仅判是否重抓）+ `AdjustCalc.Apply`（读时复权：bisect_right 找 ≤bar 的最近除权事件，
      前复权 fore/后复权 back，首事件前因子 1，乘 OHLC+preclose）。`KlineReadService` 加 adjustFlag：
      flag 1/2 → EnsureFull + 逐 bar 乘因子；`get_historical_k_data` 现 **d/w/m 全复权口径走 dotnet**
      （不再限 flag=3）。fetch_service 加 fetch_adjust_factor。复权数学有黄金单测。`dotnet test` 125/125。**未部署**。
    - ✅ **dividend（代码+单测，2026-06-25）**：`DividendParser`（典型列 + 其余字段进 detail JSONB，中文原样）+
      `DividendWriter`（upsert + 水位 last=min(年末,今天)/first=年初，detail `::jsonb`）+ `DividendService.Ensure`
      （CheckRange 按年判定）+ `DividendReadService`（detail 作嵌套 JSON `WriteRawValue` 输出）+ get_dividend_data 路由 +
      fetch_service fetch_dividend。FetchRequest 加 Year/YearType。`dotnet test` 131/131。**未部署**。
    - ✅ **财报+快报（CheckQuarter，代码+单测，2026-06-25）—— 最后一类**：两个设计关卡均解决：
      ① **payload 非表格**：fetch_service worker `_query` 重构为返回 payload；财报编码为 fields=[report_type, record]、
      每行 [类型, json(记录)]，`FinancialParser.ParseQuarterly` 拆 record 提 statDate/pubDate、其余进 metrics(JSONB)。
      ② **last_success 负结果记忆**：用 data_watermark 合成 `data_type=fin:{year}q{quarter}` 记 last_fetched_at（空结果也写）。
      `FinancialQuarterService`（CheckQuarter）+ `PerformanceService`（express/forecast 走 CheckRange）+ `FinancialWriter`
      （financial_report upsert + 两套水位）+ `FinancialReadService`（季度六类/综合指标合并/快报预告 3 种 serving）+
      9 个工具路由。`FetchRequest` 加 Quarter/ReportType。`dotnet test` 146/146。**未部署**。

  **P5 全部数据类型代码移植完成（12/12 抓取函数）**。

- ✅ **kline_minute 分区 DDL（2026-06-25）**：迁移 `PartitionKlineMinute`（手写 migrationBuilder.Sql，EF 不建模分区）
  把普通表转为按 bar_time 年度 RANGE 分区——重命名旧表→建分区父表(PK 含 bar_time)→年度分区 2023-2031+default→
  迁数据→删旧。**踩坑**：PK 索引名全局唯一，重命名表后旧约束需先 RENAME 腾名。已 apply：6688 行保留、2026 数据
  872 行正确路由到 _2026 分区、MCP 协议读 30 分钟线正常（分区对 serving 透明）。

  ## 🏁 迁移全线完成（P1–P5 + 部署 + 双向协议验证 + 分区）
  dotnet 唯一 PG 属主 + Python 无状态 baostock 抓取微服务。12/12 抓取函数、四种 coverage 模式、全部 MCP 工具 serving
  均已部署上线并经 MCP 协议实测。单测 147/147。线上：fetch(:8090)+mcp(:8000 管线ON)，旧 api 未启。

- ✅ **快照三件套 serving（2026-06-25，代码+实测）**：`SnapshotReadService`（snap_date 缺省解析最近交易日、
  stock_list 当日未发布回退前一交易日最多 4 次、PG `json_build_object` 精确列排除 updated_at）+ 9 工具路由
  （get_all_stock/search_stocks/get_suspensions、get_index_constituents/sz50/hs300/zz500、get_stock_industry/
  list_industries/get_industry_members——派生工具复用基础 JSON 过滤）。host harness 实测：stock_list 7280/sz50 50/
  industry 5530 真实落库 + serving JSON 输出正确（中文 UTF-8、按 code 序）。单测 147/147。
  **已部署 + MCP 协议实测全过**：get_sz50_stocks(50) / search_stocks(浦发) / get_suspensions(19) /
  get_stock_industry / list_industries(聚合计数) / get_industry_members(货币金融 45) 经协议返回正确。
  （mcp 重建曾卡 MCR 基础镜像慢拉 ~30min，docker 把 dotnet 基础镜像 prune 了需重拉 184MB。）

- ✅ **DateTools 派生工具（2026-06-25，已部署+协议实测）**：`TradingDaysReadService`（移植 dates.py，
  基于已迁 trade_calendar 计算，45 天回看）+ 6 工具路由（latest/is/previous/next/last_n/recent_range）。
  EnsureRange 自动把日历补到当年。MCP 协议实测全过且反映真实假期（2026-06-19 端午非交易日）。单测 147/147。

- ✅ **整体部署验证（2026-06-25）**：重建 fetch（拿到全部 fetch 类型）+ mcp（管线全开），`./up.sh fetch mcp`。
  `LiveDeployVerifyTests` 驱动各 dotnet 服务打真 fetch_service→baostock→落真 PG，**6 类型 + k_d 全部真实落库**：
  kline 6446 / kline_minute(30m) 6688 / trade_calendar 12097 / deposit_rate 41 / financial_report 6 / adjust_factor 26 /
  stock_basic 1。覆盖四种 coverage 模式 + 复权。**单测 147/147**。
  修复一个 bug：分钟线 bar_time 是 +08 DateTimeOffset，Npgsql 要求 timestamptz 参数为 UTC 偏移 → 写/读均 `.ToUniversalTime()`，
  已重建 mcp（fetch 未重启，会话保留，§0 达标）。
- ✅ **MCP 协议层 serving 实测（2026-06-25）**：最小 MCP Streamable HTTP 客户端（initialize→initialized→
  tools/call，SSE 解析）实调运行中 mcp 容器：握手成功、tools/list 44 个工具；`get_historical_k_data` 不复权 +
  **前复权(flag=2 因子生效)** + `get_trade_dates`(日历) + `get_deposit_rate_data`(空结果正确) 均经协议返回正确 JSON。
  完整链路 MCP 协议→工具→dotnet 服务(管线 ON)→直读 PG→序列化→协议返回 全程验证。serving 侧闭环。
    - ✅ **宏观 5 表（代码+单测，2026-06-25）**：EF 实体 5 个（deposit/loan 数字列名 `[Column]` 显式，
      rrr/money_supply 约定正确）+ 迁移 `AddMacroTables` 已 apply（snake_case 约定**加守卫**不覆盖 `[Column]`）。
      spec 驱动 `MacroSpecs`/`MacroParser`/`MacroWriter`（移植 _MACRO_SPECS，5 类共用）+ `MacroService`
      （利率类 ISO 日期、货币供应 YYYY-MM/YYYY，水位折 date）+ `MacroReadService`（PG `json_agg` 通用序列化
      全列，免逐表手写）+ 5 个工具路由。fetch_service fetch_macro。`dotnet test` 137/137。**未部署**。
    - ✅ **分钟线 k_5/15/30/60（代码+单测，2026-06-25）**：`KlineMinute` 实体（**普通表**，分区 DDL 暂不做）
      + 迁移 `AddKlineMinute` 已 apply。`KlineMinuteParser`（bar_time 取 "time" 前 14 位 +08）+ `KlineMinuteWriter`
      （ON CONFLICT + 水位）+ `KlineMinuteService`（与 KlineService 同构，频率 int，切片 730，**复用 fetch_kline**
      ——provider.query_k_data 对分钟频率同样适用，无需改 fetch_service）+ `KlineMinuteReadService`（json_agg，
      bar_time 右开区间 +08）+ get_historical_k_data 分钟分支路由。`dotnet test` 142/142。**未部署**。
      遗留：kline_minute 旧库按 bar_time RANGE 年度分区（性能），此处普通表，后续可加分区 DDL。
  - ⬜ **fetch_service 多类型**：worker `_query` 现仅 fetch_kline，需加 fetch_adjust_factor/
    fetch_trade_calendar/fetch_dividend/fetch_stock_basic/财报/快照（provider.query_* 分发）。
  - ⬜ **宏观 5 表**（deposit_rate/loan_rate/required_reserve_ratio/money_supply_month/year）：
    列名带数字（3month/m0/m1）snake_case 约定不可靠，需显式 [Column]，单独一轮。
  - ⬜ **分钟线**（k_5/15/30/60）：KlineMinute 表按 bar_time RANGE 分区，EF 迁移需自定义分区 DDL，
    单独处理（或先建普通表）。
- ⬜ **P6 beat 移植到 dotnet** ⚠️ landmine
  beat 现靠读 PG 决定调度（`_is_trading_day` 查日历、`sync_tracked_codes` 遍历水位），
  PG 只在 dotnet 后**只能在 dotnet 跑**（Quartz.NET / Hangfire / BackgroundService+cron）。
  4 个任务（日历 08:00 / 昨日列表 08:30 / 市场 17:00 / 已入库代码 17:10 + 交易日 gating）
  整体移植，读 PG 决定 → 调 `POST /fetch`（复用去重）。
- ✅ **P7 Python 瘦身（2026-06-25）**
  删 `api/`、`db/`、`fetcher/{tasks,app,beat,worker,writer}.py`、`core/{timeutil,helpers}.py`、`tests/`、`deploy/`、
  `alembic.ini`——Python 仅剩 11 文件（fetch_service + providers + core.ratelimit + settings）。pyproject 依赖
  41→**24 包**（删 sqlalchemy/alembic/celery/psycopg，celery 原带 redis 改直接依赖），`uv lock` 重锁。
  **连带修复**：删 api 后发现部分 mcp 工具仍靠旧 api 取数（k_d 切换后已坏）——全部迁到 dotnet：
  `KlineLoader` 改读 `KlineReadService`（修复 rsi/obv/cci/dual_ma/ma_alignment/vegas 6 个指标工具）、
  `UtilTools` 用 CodeNormalizer+指数映射（纯函数）、`get_adjust_factor_data`→`AdjustFactorReadService`、
  `get_stock_analysis`→`StockAnalysisService`（港 Markdown 报告，聚合基本/行业/财报/K线）。
  compose 删 migrate/api 服务（只剩 fetch+mcp，镜像 stockdata-fetch）。重建两容器，MCP 协议实测全过。单测 148/148。
  **配置/文档清理**：`settings.py` 砍到仅 fetch 所需 8 项（删 pg_dsn/broker/result/visibility/wait/minute_backfill），
  `baostock.py` 去掉 broker_url 兜底；`.env`/`.env.example` 删 Celery broker/result、加 FETCH_JOB_REDIS_URL、重排注释；
  删 `server/docs/`（本次迁移前那轮重构的过时文档，引用已删代码）；根 `README.md` 与 `server/README.md` 重写为
  新架构（dotnet 属主 + Python fetch 微服务，含「fetch 如何工作」）。配置变更随下次 fetch 重建生效，现网容器不受影响。
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
