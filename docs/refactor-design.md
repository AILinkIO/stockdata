# 重构设计文档：MCP 服务 → Web 服务 + Celery 任务体系

> 状态：已实施完成（含可选阶段 7 MCP 薄壳）。旧实现见 tag `pre-restructure`
> 关联文档：
> - [migration-plan.md](migration-plan.md) —— 迁移实施计划（各阶段任务清单、验收标准、回滚预案）
> - [cache-strategy.md](cache-strategy.md) —— 旧缓存策略，本文档 5.4 节为其 PostgreSQL 化映射，迁移完成后归档

## 1. 背景与目标

### 1.1 现状痛点

当前项目是基于 FastMCP 的 A 股数据 MCP 服务器，存在两个结构性问题：

1. **baostock 的模块级全局 TCP 连接**：不支持并发、无法设置 socket 超时、底层阻塞在
   C socket 上时线程无法被打断。现有方案（`providers/context.py` 单 worker 线程串行化
   + execute 超时保护）只能"放弃等待"，无法回收挂死的连接本身——这是历史上 worker
   线程挂死问题（commit `1ff1d8e`、`7aa2ba0`）的根因。
2. **diskcache 缓存模型的局限**：以查询参数为 key 缓存 DataFrame blob，数据不可查询、
   不可组合、无法做增量管理。

### 1.2 重构目标

1. baostock 数据获取改为 **消息队列 + 多进程** 任务体系（Celery，broker 用
   Redis，比 RabbitMQ 更轻量），置于 `fetcher/` 目录。
2. fetcher 接收任务后在**子进程**中执行抓取，回收数据后**杀死子进程**——超时挂死时
   SIGKILL 是 100% 有效的兜底手段。
3. 使用 **PostgreSQL** 进行数据管理：领域化的数据仓库 + 覆盖度元数据，替代 blob 缓存。
4. 不再是 MCP：使用 **FastAPI** 作为常驻 Web 服务，**SQLAlchemy 2.0** 做 ORM 映射。

### 1.3 现有资产去向

| 模块 | 现状 | 去向 |
|---|---|---|
| `providers/interface.py` | 22 个抽象方法的数据源接口 | 保留，作为 fetcher 内部 provider 抽象 |
| `providers/baostock.py` | 纯查询逻辑 | 几乎原样复用，搬入 fetcher 子进程内执行 |
| `providers/context.py` | worker 线程串行化机制 | **删除**，被进程隔离取代 |
| `providers/cache.py` | diskcache + TTL 策略 | **删除**，策略转化为水位表新鲜度规则（5.4 节） |
| `tools/*`（8 个模块） | MCP 工具定义 | 转化为 FastAPI 路由 |
| `services/validation.py` | 参数校验 | 吸收进 Pydantic schema |
| `services/tool_runner.py` | 异常统一处理 | 转化为 HTTP 异常映射（6.3 节） |
| `core/*` | 分析逻辑、日期工具 | 原样保留 |
| `formatting/markdown.py` | Markdown 输出 | 可选保留（报告类接口），常规接口返回 JSON |
| `main.py` / `server.py` | MCP 入口 | 删除 |

## 2. 总体架构

```
                    ┌──────────────┐
 HTTP client ──────▶│  api/ FastAPI │──读──▶ PostgreSQL ◀──写──┐
                    │  (uvicorn)    │                          │
                    └──────┬───────┘                          │
                  缺数据时投递任务│                                │
                           ▼                                  │
                    Redis(队列) ◀── beat 定时同步任务            │
                           │                                  │
                    ┌──────┴───────┐    fork    ┌─────────────┴──┐
                    │ fetcher/      │──────────▶│ 子进程          │
                    │ celery worker │  超时SIGKILL│ bs.login()     │
                    │ (父进程监督)   │◀──────────│ 查询→DataFrame  │
                    └──────────────┘   回收结果  │ →落库→退出/复用 │
                                                └────────────────┘
```

三个 Python 常驻进程 + 两个基础设施。不使用 Docker：PostgreSQL 与 Redis 通过
系统包管理器本机安装，三个 Python 进程由 systemd 单元管理（开发期直接 `uv run`）：

| 进程 | 职责 |
|---|---|
| `api`（uvicorn + FastAPI） | 对外 HTTP 接口；查水位表决策"直接读库"或"投递任务" |
| `fetcher`（celery worker，prefork） | 消费队列任务；子进程抓取 → 落库 → 更新水位 |
| `beat`（celery beat） | 定时投递同步任务（交易日历、股票列表、成分股等） |
| Redis 协议服务（实际为系统已有的 Valkey 8） | Celery broker + 结果后端（db0/db1 已被其他应用占用，本项目用 db2=broker、db3=结果） |
| PostgreSQL 17 | 数据仓库 |

### 2.1 请求数据流（读穿透）

1. API 收到查询 → 查 `data_watermark`（一次主键点查）判断覆盖度与新鲜度；
2. 已覆盖且新鲜 → 直接从事实表读取返回；
3. 缺失/过期 → 投递抓取任务到队列 → 等待任务完成（默认超时 60s）→ 读库返回；
4. 等待超时 → 返回 504，任务继续在后台执行，客户端可稍后重试（届时已落库）。

## 3. 技术选型

| 维度 | 选型 | 理由 |
|---|---|---|
| 任务系统 | **Celery 5.x（prefork 池）** | 即"消息队列 + 多进程"的标准实现：`worker_max_tasks_per_child` 给出"处理 N 个任务后杀死子进程"语义；`task_time_limit` 硬超时 SIGKILL 挂死子进程；`worker_process_init` 信号用于子进程 fork 后登录 baostock。监督逻辑无需手写 |
| 队列后端 | **Redis 7.x**（broker + 结果后端） | 比 RabbitMQ 轻量得多（单二进制、零配置可用），broker 与结果后端一个组件全包；本系统任务可重建（读穿透会重新投递、落库幂等），不需要 RabbitMQ 级别的投递可靠性 |
| Web 框架 | **FastAPI** | 异步、Pydantic v2 原生校验、自动 OpenAPI 文档；与现有 uvicorn 运维心智一致 |
| ORM | **SQLAlchemy 2.0（async）+ Alembic** | 行业标准；金融宽表 + 复合唯一键表达力最稳；Alembic 管 schema 演进 |
| PG 驱动 | **psycopg3**（API 侧 async，任务侧 sync） | 一个驱动两种模式；对新 Python 版本跟进最快 |
| 配置 | pydantic-settings | 与 FastAPI 同生态，env 驱动 |
| 数据库 | PostgreSQL 17 | 需求指定；分钟线按年分区 |
| 部署 | 本机直装 + systemd | PG/Redis 走系统包管理器；api / fetcher / beat 三个 systemd 单元，开发期 `uv run` |

**落选方案**：

- RabbitMQ 作 broker：投递可靠性更强（持久化队列、publisher confirm），但多一个重组件；
  本系统任务幂等可重建，用不到这级保证。
- 手写队列消费 + multiprocessing：完全可控，但消息确认、重连、僵尸进程、优雅退出全部要
  自己处理——线程版已经踩过这类坑。
- Dramatiq：更轻，但其 TimeLimit 是线程内抛异常，**打不断挂死的 C socket**，不满足
  核心需求。
- SQLModel / Tortoise：封装层或迁移工具弱，不如直接用 SQLAlchemy。
- Django + DRF：本项目无 admin/auth 诉求，过重。

## 4. fetcher 设计

### 4.1 子进程生命周期

Celery 配置即可完整表达需求中的"创建子进程 → 抓取 → 回收数据 → 杀死子进程"：

```python
broker_url = "redis://localhost:6379/2"     # 共享 Valkey 实例，db0/db1 属于其他应用
result_backend = "redis://localhost:6379/3"
result_expires = 600              # 结果只为读穿透等待服务，10 分钟足够

worker_concurrency = 3            # 子进程数。baostock 服务端是串行瓶颈，2~4 足够
worker_max_tasks_per_child = 20   # 处理 N 个任务后杀死回收子进程（=1 即严格一任务一进程）
task_time_limit = 90              # 硬超时：SIGKILL 子进程，治挂死
task_soft_time_limit = 60         # 软超时：先抛异常给任务清理机会
task_acks_late = True             # 子进程被 kill 后任务重回队列重试
task_reject_on_worker_lost = True

# Redis broker 下 acks_late 的重投递依赖 visibility_timeout：
# 任务超过该时长未 ack 即被重新投递。必须 > task_time_limit，否则长任务会被重复执行
broker_transport_options = {"visibility_timeout": 600}
```

- **登录时机**：`worker_process_init` 信号中执行 `bs.login()`——即 fork 之后、子进程
  内部。父进程永不接触 baostock，保证每个子进程持有独立连接。
- **登录成本权衡**：baostock login 约 1~2s。`worker_max_tasks_per_child=1` 是需求字面
  语义（每任务一进程），但每任务背 1~2s 固定开销且高频登录有触发服务端限制的风险；
  设为 20~50 可摊薄登录成本，超时挂死时照样被 SIGKILL。**默认 20，做成配置项。**
- **幂等性**：所有落库使用 `INSERT ... ON CONFLICT DO UPDATE`（5.1 节自然主键），
  任务被 kill 后重试天然安全。

### 4.2 任务类型

按数据集分组（而非 22 个接口方法各一个任务），任务内完成：查询 → 解析（字符串 →
NUMERIC/日期）→ upsert 落库 → 更新 `data_watermark`。

| 任务 | 覆盖接口 | 写入表 |
|---|---|---|
| `fetch_kline` | get_historical_k_data（d/w/m） | `kline` |
| `fetch_kline_minute` | get_historical_k_data（5/15/30/60） | `kline_minute` |
| `fetch_adjust_factor` | get_adjust_factor_data | `adjust_factor` |
| `fetch_stock_basic` | get_stock_basic_info | `stock_basic` |
| `fetch_dividend` | get_dividend_data | `dividend` |
| `fetch_financial_report` | 六类季度财报 + 快报 + 预告 | `financial_report` |
| `fetch_trade_calendar` | get_trade_dates | `trade_calendar` |
| `fetch_stock_list` | get_all_stock | `stock_list_snapshot` |
| `fetch_index_constituent` | sz50 / hs300 / zz500 | `index_constituent` |
| `fetch_industry` | get_stock_industry | `stock_industry` |
| `fetch_macro` | 5 个宏观接口 | 宏观各表 |

### 4.3 任务追踪与去重

`fetch_task` 表（5.3 节）记录每个任务的参数、状态与时间戳，用于观测与排障；
pending/running 状态的任务按参数哈希做部分唯一索引，避免同一查询在落库前被重复入队。

### 4.4 定时同步（beat）

| 任务 | 调度 | 说明 |
|---|---|---|
| 交易日历 | 每日 08:00 | 当年日历 |
| 全部股票列表 | 每交易日 17:00 | 新股/退市 |
| 指数成分股 ×3 | 每交易日 17:30 | 季度调整捕获 |
| 热门标的日 K 预热 | 每交易日 17:30 | 可选，按访问统计圈定 |

## 5. 数据库设计

### 5.1 设计原则

1. **业务时间与系统时间永远分列**：`trade_date` / `divid_operate_date` / `stat_date`
   是数据"属于"哪天；`updated_at` / `last_fetched_at` 是我们哪天"知道"的。所有新鲜度
   判断建立在这对区分上。
2. **只存原始事实，派生数据读时计算**：K 线只存**不复权**数据 + 复权因子序列，
   前/后复权价在读取时计算；综合财务指标由六类财报读时合并。失效问题从根上消失——
   除权事件发生时仅因子表多一行，K 线数据零失效。
3. **自然复合主键 + upsert**：事实表一律 `(实体, 类型, 业务时间)` 复合主键，不设自增
   id。时序数据没有更新自然键的场景，省一个索引、天然防重、任务重试幂等。
4. **水位表是唯一的抓取决策入口**：API 层不在事实表上扫 `max(date)`，一次主键点查
   决定"直接读库"还是"投递任务"。
5. **类型**：价格 NUMERIC（baostock 返回 4 位小数字符串，精确入库，不用 float）、
   成交量 BIGINT、成交额 NUMERIC(20,4)、时间戳 timestamptz。

### 5.2 核心表

#### 5.2.1 除权因子 `adjust_factor` —— 每个除权除息事件一行（多条）

复权价的计算需要**完整的因子序列**而非最新值，因此必须多条。"因子变没变"用
`max(divid_operate_date)` 即可回答，旧缓存的 fingerprint（行数 + 末行日期 + 末行因子）
在此结构上是一条简单聚合查询，无需单独存储。

```sql
CREATE TABLE adjust_factor (
    code               varchar(12)   NOT NULL,
    divid_operate_date date          NOT NULL,   -- 除权除息日（业务时间，因子变化时间）
    fore_adjust_factor numeric(18,8) NOT NULL,   -- 前复权因子
    back_adjust_factor numeric(18,8) NOT NULL,   -- 后复权因子
    adjust_factor      numeric(18,8),            -- 本次复权因子
    updated_at         timestamptz   NOT NULL DEFAULT now(),  -- 系统时间：何时获知/更新
    PRIMARY KEY (code, divid_operate_date)
);
```

写入语义：新除权事件 = 新增一行（`updated_at` 记录获知时刻）；交易所修正历史因子
（罕见）= `ON CONFLICT DO UPDATE` 覆盖已有行。

#### 5.2.2 数据水位表 `data_watermark`

记录每个 (code, 数据类型) 的数据覆盖区间与抓取时刻，是读穿透的决策依据。

```sql
CREATE TABLE data_watermark (
    code            varchar(12) NOT NULL DEFAULT '',  -- 全市场数据集（日历、宏观）用空串
    data_type       varchar(24) NOT NULL,  -- 'k_d'/'k_w'/'k_m'/'k_5'/'k_15'/'k_30'/'k_60'
                                           -- /'adjust_factor'/'profit'/.../'dividend'/...
    first_date      date,                  -- 已持有数据的起始日
    last_date       date        NOT NULL,  -- 业务水位：数据已更新到的那一天
    last_fetched_at timestamptz NOT NULL,  -- 系统水位：最后一次抓取时刻
    PRIMARY KEY (code, data_type)
);
```

两个关键设计决定：

- **双水位**："数据更新到了今天"≠"数据是新鲜的"。盘中今日 bar 被反复覆写时
  `last_date` 不变、`last_fetched_at` 在变。新鲜度规则见 5.4 节。
- **以策略保证连续，杜绝空洞**：单行水位表达不了"抓了 2020 但没抓 2015"的洞。
  解法是**首次触达某 code 时从上市日一次性回填到今天**（日线全史几千行，一次 API
  调用即回；分钟线从配置的保留起点回填）。覆盖范围即干净的 `[first_date, last_date]`
  闭区间。
- `data_type` 覆盖**所有**数据集（K 线、因子、财报、分红、成分股……），枚举值在
  Python 层用 Enum 管理，库里存 varchar（PG 原生 enum 加值需 DDL，不值得）。

#### 5.2.3 日/周/月 K 线 `kline`

只存不复权数据（无 adjust_flag 列）；估值字段仅日线有值，周/月为 NULL。

```sql
CREATE TABLE kline (
    code         varchar(12)  NOT NULL,
    frequency    char(1)      NOT NULL,           -- 'd' / 'w' / 'm'
    trade_date   date         NOT NULL,           -- 数据日期（业务时间）
    open         numeric(12,4),
    high         numeric(12,4),
    low          numeric(12,4),
    close        numeric(12,4),
    preclose     numeric(12,4),                   -- 仅日线
    volume       bigint,
    amount       numeric(20,4),
    turn         numeric(10,6),
    pct_chg      numeric(10,6),
    trade_status smallint,                        -- 仅日线
    is_st        boolean,                         -- 仅日线
    pe_ttm       numeric(14,6),                   -- 仅日线，估值四件套
    pb_mrq       numeric(14,6),
    ps_ttm       numeric(14,6),
    pcf_ncf_ttm  numeric(14,6),
    updated_at   timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (code, frequency, trade_date)
);
```

- 周/月线直接存 baostock 返回值（与数据源对账方便），不从日线推导。
- 复权价读取时计算：`SELECT k.*, f.fore_adjust_factor FROM kline k LEFT JOIN
  adjust_factor f ON ...`，在应用层按因子序列乘算（沿用 baostock 复权口径）。

#### 5.2.4 分钟 K 线 `kline_minute`

与日线分表的理由：时间轴是 timestamp 不是 date、无估值字段、数据量大两个数量级
需要独立分区。

```sql
CREATE TABLE kline_minute (
    code       varchar(12)  NOT NULL,
    frequency  smallint     NOT NULL,            -- 5 / 15 / 30 / 60
    bar_time   timestamptz  NOT NULL,            -- bar 结束时刻
    open       numeric(12,4),
    high       numeric(12,4),
    low        numeric(12,4),
    close      numeric(12,4),
    volume     bigint,
    amount     numeric(20,4),
    updated_at timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (code, frequency, bar_time)
) PARTITION BY RANGE (bar_time);   -- 按年分区，Alembic 迁移中预建若干年分区
```

#### 5.2.5 财报表 `financial_report` —— 单表 + 类型 + JSONB

baostock 财务数据 8 类（盈利/营运/成长/偿债/现金流/杜邦/业绩快报/业绩预告），各类
指标列完全不同（合计 60+）。指标列方案对比：

| 方案 | 优点 | 缺点 |
|---|---|---|
| 8 张类型化表 | 列有类型，SQL 可直接截面筛选 | 表多；数据源加字段需 migration |
| 单表 + 60 个可空列 | 一张表 | 每行 7/8 的列恒为 NULL |
| **单表 + JSONB（采用）** | 一张表；schema 弹性；与"按 code+季度取全部指标渲染"的消费模式完全匹配 | 截面筛选需 JSONB 表达式 |

现有全部消费场景都是"按 (code, 报告期) 取整行、全字段渲染"，无跨股票指标筛选，
故采用 JSONB。未来若做选股筛选器，将高频指标提升为生成列即可
（`GENERATED ALWAYS AS ((metrics->>'roeAvg')::numeric) STORED`），无需推倒重来。

**"财报日期"必须拆成两个**：`stat_date`（报告期，数据归属的会计期间，主键成分）与
`pub_date`（披露日期）。同一报告期可能因更正而重新披露——`stat_date` 不变、
`pub_date` 与指标值变，upsert 自动覆盖为最新版本。

```sql
CREATE TABLE financial_report (
    code        varchar(12) NOT NULL,
    report_type varchar(20) NOT NULL,  -- profit/operation/growth/balance/cash_flow
                                       -- /dupont/express/forecast
    stat_date   date        NOT NULL,  -- 报告期（季度可推导，不单存）
    pub_date    date,                  -- 披露日期
    metrics     jsonb       NOT NULL,  -- 该类型全部指标
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (code, report_type, stat_date)
);
```

综合财务指标（原 `get_fina_indicator`）= `WHERE code = ? AND stat_date = ?` 一次查询
后应用层按 `report_type` 前缀合并，取代原先六次缓存查找的组装逻辑。

### 5.3 支撑表

```sql
-- 股票基本信息（query_stock_basic）
CREATE TABLE stock_basic (
    code       varchar(12) PRIMARY KEY,
    code_name  varchar(64),
    ipo_date   date,
    out_date   date,
    type       smallint,        -- 1 股票 / 2 指数 / 3 其它
    status     smallint,        -- 1 上市 / 0 退市
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- 分红送转（query_dividend_data）。预案公告日恒存在，作主键成分；
-- 全部日期/比例字段保留为类型化列，便于与除权因子对账
CREATE TABLE dividend (
    code                    varchar(12) NOT NULL,
    plan_announce_date      date        NOT NULL,  -- 预案公告日
    year                    smallint    NOT NULL,  -- 查询归属年份
    year_type               char(6)     NOT NULL,  -- report/operate
    regist_date             date,                  -- 股权登记日
    operate_date            date,                  -- 除权除息日
    pay_date                date,                  -- 派息日
    cash_ps_before_tax      numeric(12,6),
    cash_ps_after_tax       numeric(12,6),
    stocks_ps               numeric(12,6),
    reserve_to_stock_ps     numeric(12,6),
    detail                  jsonb,                 -- 其余低频字段
    updated_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (code, plan_announce_date, year_type)
);

-- 交易日历
CREATE TABLE trade_calendar (
    calendar_date  date PRIMARY KEY,
    is_trading_day boolean NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- 全部股票列表快照（query_all_stock，按需抓取的日期快照）
CREATE TABLE stock_list_snapshot (
    snap_date    date        NOT NULL,
    code         varchar(12) NOT NULL,
    code_name    varchar(64),
    trade_status boolean,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (snap_date, code)
);

-- 指数成分股（sz50 / hs300 / zz500）
CREATE TABLE index_constituent (
    index_code  varchar(8)  NOT NULL,   -- 'sz50' / 'hs300' / 'zz500'
    snap_date   date        NOT NULL,   -- 查询基准日
    code        varchar(12) NOT NULL,
    code_name   varchar(64),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (index_code, snap_date, code)
);

-- 行业分类
CREATE TABLE stock_industry (
    snap_date               date        NOT NULL,
    code                    varchar(12) NOT NULL,
    code_name               varchar(64),
    industry                varchar(64),
    industry_classification varchar(64),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (snap_date, code)
);

-- 宏观数据：低频小表，按 baostock 字段类型化建列，主键为发布/生效日期（+子类型）
--   deposit_rate(pub_date, deposit_type, rate, ...)            PK (pub_date, deposit_type)
--   loan_rate(pub_date, loan_type, rate, ...)                  PK (pub_date, loan_type)
--   required_reserve_ratio(pub_date, effective_date, ...)      PK (pub_date, effective_date)
--   money_supply_month(stat_year, stat_month, m0/m1/m2 各值)    PK (stat_year, stat_month)
--   money_supply_year(stat_year, m0/m1/m2 年末余额)             PK (stat_year)
-- 均含 updated_at 列，结构简单，实施阶段照 baostock 返回字段直接落列

-- 抓取任务追踪
CREATE TABLE fetch_task (
    id             bigserial PRIMARY KEY,
    task_type      varchar(32) NOT NULL,
    params         jsonb       NOT NULL,
    params_hash    varchar(64) NOT NULL,   -- 参数规范化哈希
    status         varchar(12) NOT NULL DEFAULT 'pending',
                   -- pending / running / succeeded / failed
    celery_task_id varchar(64),
    error          text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    started_at     timestamptz,
    finished_at    timestamptz
);
-- 去重：进行中的同参数任务只允许一个
CREATE UNIQUE INDEX uq_fetch_task_active ON fetch_task (params_hash)
    WHERE status IN ('pending', 'running');
```

### 5.4 新鲜度规则（cache-strategy.md 的水位表映射）

读穿透时对 `data_watermark` 行的判定规则。通用形式：

- **请求范围 ⊄ [first_date, last_date]** → 投递抓取任务补齐缺口；
- **请求范围 ⊆ 区间且区间尾不含"今天"** → 直接读库（历史数据永久有效，原 PERMANENT）；
- **区间尾含今天** → 按下表的刷新间隔比对 `last_fetched_at`。

| data_type | 含今天时的刷新间隔 | 历史部分 | 对应旧策略 |
|---|---|---|---|
| `k_d` / `k_5`~`k_60` | 5 分钟 | 永久 | 已完结月份永久 / 当月 5 分钟 |
| `k_w` | 5 分钟（本周未完）| 永久（< 本周一） | 周线按周定型 |
| `k_m` | 5 分钟（本月未完）| 永久（< 本月 1 日） | 月线按月定型 |
| `adjust_factor` | 5 分钟 | 永久 | 同旧策略 |
| `stock_basic` | 1 天 | — | 同旧策略 |
| `dividend` | 1 天（当年） | 永久（往年） | 同旧策略 |
| 六类季度财报 | 1 天（披露截止日未过） | 永久（截止日已过：Q1→4/30、Q2→8/31、Q3→10/31、Q4→次年4/30） | `_is_past_quarter` |
| `express` / `forecast` | 1 天 | 永久 | 同旧策略 |
| `trade_calendar` | 1 天 | 永久 | 同旧策略 |
| `stock_list` | 1 天 | 永久 | 同旧策略 |
| 宏观 5 类 | 7 天（近 2 个月内） | 永久（2 个月前，发布滞后已消化） | `_is_macro_settled` |
| `stock_industry` | 7 天 | 永久 | 同旧策略 |
| `index_constituent` | 1 天 | 永久 | 同旧策略 |

旧策略中两项不再需要：

- **按月拆分缓存** → 数据已按行存储，任意日期范围天然可组合；"合并连续缺失月份为
  最少 API 调用"退化为"对水位缺口区间一次抓取"。
- **复权因子 fingerprint** → 不存复权价，无失效问题（5.1 原则 2）。

规则实现集中在 `db/coverage.py`，单元测试重点覆盖。

## 6. API 设计

### 6.1 路由映射（tools/ → routers/）

| 现 MCP 工具模块 | REST 路由前缀 | 端点示例 |
|---|---|---|
| `stock_market` | `/api/v1/stocks` | `GET /{code}/kline`、`/{code}/basic`、`/{code}/dividends`、`/{code}/adjust-factors` |
| `financial_reports` | `/api/v1/stocks` | `GET /{code}/financials/{report_type}`、`/{code}/financials/indicator` |
| `indices` | `/api/v1/indices` | `GET /{index_code}/constituents`、`/api/v1/industries` |
| `market_overview` | `/api/v1/market` | `GET /trade-calendar`、`/stocks` |
| `macroeconomic` | `/api/v1/macro` | `GET /deposit-rate`、`/loan-rate`、`/rrr`、`/money-supply` |
| `date_utils` | `/api/v1/dates` | `GET /latest-trading-day`、`/is-trading-day` |
| `analysis` | `/api/v1/stocks` | `GET /{code}/analysis?type=fundamental|technical|all` |
| `helpers` | `/api/v1/utils` | `GET /normalize-code` |

- 常规端点返回 JSON（Pydantic 响应模型）；`analysis` 报告类端点可带
  `Accept: text/markdown` 复用 `formatting/markdown.py`。
- 批量回填走独立异步接口：`POST /api/v1/tasks/backfill` → 202 + task_id，
  `GET /api/v1/tasks/{id}` 查询进度（读 `fetch_task` 表）。

### 6.2 读穿透实现注意

async 路由中等待 Celery 结果**不可阻塞事件循环**：用 `AsyncResult` 轮询
（`asyncio.sleep` 间隔 0.5s）或丢进 executor，总超时默认 60s（与
`task_soft_time_limit` 对齐）。

### 6.3 异常 → HTTP 映射（取代 tool_runner.py）

| 异常 | HTTP | 说明 |
|---|---|---|
| 参数校验失败 | 422 | Pydantic 自动处理 |
| `NoDataFoundError` | 404 | 查询范围无数据 |
| 等待抓取超时 | 504 | 任务继续后台执行，稍后重试即命中 |
| `LoginError` / `DataSourceError` | 502 | 上游数据源故障 |
| 未预期异常 | 500 | 记日志，响应不暴露细节 |

## 7. 目录结构

```
stockdata/
├── api/                  # FastAPI 应用
│   ├── main.py
│   ├── routers/          # 8 个路由模块，一一对应现 tools/
│   ├── schemas/          # Pydantic 请求/响应模型（吸收 services/validation.py）
│   └── services/         # 读穿透编排、复权计算、财报合并
├── fetcher/              # Celery 应用
│   ├── app.py            # Celery 实例 + 子进程生命周期配置（4.1 节）
│   ├── tasks.py          # 任务定义（4.2 节）
│   ├── beat.py           # 定时调度（4.4 节）
│   ├── providers/        # 现 src/providers/{interface,baostock}.py 迁入（删 context.py）
│   └── writer.py         # DataFrame 解析 → 批量 upsert
├── db/
│   ├── models/           # SQLAlchemy 2.0 模型（第 5 章）
│   ├── coverage.py       # 新鲜度/覆盖度规则（5.4 节）
│   ├── session.py        # async/sync 双引擎
│   └── alembic/
├── core/                 # 现 src/core/ 原样保留
├── settings.py           # pydantic-settings
└── deploy/               # systemd 单元文件（stockdata-api / -fetcher / -beat.service）
```

## 8. 迁移计划

详细实施步骤（各阶段任务清单、验收操作、风险分支、回滚预案）见
[migration-plan.md](migration-plan.md)，此处为概览：

| 阶段 | 内容 | 工期 | 验收标准 |
|---|---|---|---|
| 0. 基础设施 | 本机安装 PG17 + Redis7；uv 加新依赖；**Python 3.14 + Celery prefork 冒烟测试** | 0.5 天 | worker 能起、任务能跑；不通过则启用备选方案（9.1） |
| 1. 数据模型 | 第 5 章全部表的 SQLAlchemy 模型 + Alembic 首个迁移 | 1~2 天 | `alembic upgrade head` 建出全部表与索引 |
| 2. fetcher | Celery 应用 + 子进程生命周期配置；`providers/baostock.py` 迁入并包装为任务；落库 + 水位更新；**删除 context.py 线程机制** | 2~3 天 | 三条链路验证：正常路径、超时 SIGKILL 路径（人为 sleep）、子进程回收路径（max_tasks_per_child） |
| 3. 覆盖度服务 | 5.4 节规则实现为 `db/coverage.py` | 1~2 天 | 规则单元测试全绿 |
| 4. API | 8 个 router + 读穿透编排 + 异常映射 | 2~3 天 | OpenAPI 文档完整；端到端"空库 → 触发抓取 → 返回数据"打通 |
| 5. 定时同步 | beat 调度（4.4 节） | 0.5~1 天 | 日历/列表/成分股按时落库 |
| 6. 对照切换 | 抽样 20~30 组查询参数，旧 MCP 工具 vs 新 REST 接口比对数据一致性；打 tag 后删除 MCP 代码与 fastmcp/diskcache 依赖；更新 README | 1~2 天 | 对照脚本零差异；旧代码已删 |
| 7.（可选）MCP 薄壳 | 纯转发 REST API 的 MCP 客户端，供 LLM 使用 | 0.5 天 | — |

总计 **8~13 个工作日**。关键路径：阶段 0 的兼容性验证、阶段 2/3。

## 9. 风险与待验证项

### 9.1 Python 3.14 与 Celery 兼容性（阶段 0 必须验证）

项目 `requires-python >= 3.14`，Celery（及其 billiard 多进程库）对新 Python 版本适配
历史上偏慢。阶段 0 第一件事是在 3.14 下跑 prefork worker 冒烟测试。不通过的降级路线：

1. 放宽 `requires-python` 到 3.13；
2. 或退回手写 Redis 队列消费（`BRPOP`）+ `multiprocessing(spawn)` 方案
   （监督逻辑自实现）。

### 9.2 baostock 登录频率

`worker_max_tasks_per_child=1` 时高频登录可能触发服务端限制。默认 20 摊薄；
若仍出现登录失败，调大该值并降低 `worker_concurrency`。

### 9.3 分钟线数据量

全市场 30/60 分钟线全史约数亿行。策略：按需抓取（不做全市场分钟线预热）+
配置分钟线回填起点（如近 3 年）+ 按年分区便于滚动清理。

### 9.4 已定的默认决策（可配置调整）

| 决策点 | 默认值 |
|---|---|
| 子进程回收粒度 | `worker_max_tasks_per_child = 20`（=1 即严格一任务一进程） |
| API 等待语义 | 读穿透 + 60s 超时；批量回填走异步任务接口 |
| 财报指标存储 | JSONB；需截面筛选时提升生成列 |
| 周/月线来源 | 直接存 baostock 返回值，不从日线推导 |

## 10. 实施偏差记录（as-built）

实施期间确定的与本文档原始设计的偏差，以下为最终实现状态：

1. **队列后端为系统已有的 Valkey 8**（Debian 13 以 Valkey 替代 Redis，协议兼容）。
   实例与其他应用共享：db0 被占用，本项目用 db2=broker、db3=结果后端；
   **未开启 AOF**（不动共享实例的持久化配置，任务可重建可接受）。
2. **API 路由为同步函数**（FastAPI 自动调度到线程池），数据访问用同步
   SQLAlchemy session——读穿透中阻塞等待 Celery 结果不影响事件循环，
   且实现显著简化。原设计的 async engine 未启用（db/session.py 中保留）。
3. **季度财报放弃区间水位**：点状抓取配区间水位会对中间未抓取的季度产生虚假
   覆盖声明。改用"financial_report 行存在性 + fetch_task 成功记录"做点状判定
   （fetch_task 兼作披露期后空结果的永久负记忆），见 `db/coverage.py::check_quarter`。
   "水位表是唯一抓取决策入口"（5.1 原则 4）对季度财报与快照类数据集放宽为
   "水位表/事实表存在性 + fetch_task"。
4. **express/forecast 的日期过滤字段为披露日期 pub_date**（与 baostock 查询语义
   实测一致），而非报告期 stat_date。
5. **闲置连接预防性重登录**：baostock 服务端会静默断开闲置连接（实测约数分钟），
   复用死连接会阻塞 recv 直到被 SIGKILL。provider 在连接闲置超 60s 时主动重登录，
   且重登录路径不调用 logout（旧连接的 logout 同样会挂）。
6. **僵尸任务防护**：子进程被 SIGKILL 时任务内的状态标记无法执行，fetch_task 行
   会卡在 running 并永久占住去重索引。dispatch 端在 Celery 结果失败时兜底标记
   failed；等待端对 started_at 超过 task_time_limit×2 的 running 行判僵并清理。
7. **当日股票列表盘中未发布**：fetch_stock_list 对空结果返回 0 行不记水位，
   API 层在未显式指定日期时自动回退前一交易日（最多 3 个）。
8. **未来日期范围钳制**：除交易日历外，覆盖度判定将请求尾钳制到今天，
   避免含未来日期的请求每次都判出尾部缺口、空投任务。
9. **复权公式（实测确认）**：bar 复权价 = 不复权价 × 因子，因子取除权日 ≤ bar
   日期的最近一次事件（前复权用 fore、后复权用 back），首个事件之前因子为 1。
   与 baostock 输出逐位一致。
10. **阶段 7（MCP 薄壳）曾实施后又移除**：实施记录见 commit `8eee679`，
    后经决策（2026-06-11）彻底去 MCP 化——任务提交与数据获取统一走 REST
    （读穿透 + `/api/v1/tasks/backfill`）。如需找回薄壳实现，检出该 commit。
11. **队列分片亲和（2026-06-11 新增需求）**：任务按 `crc32(任务名:code) % worker_shards`
    路由到分片队列，每个分片由一个单进程 worker（`-Q shardN -c 1`）消费——
    同 code 同任务类型恒定落在同一进程，天然串行（不会被并发抓取）并复用该进程的
    baostock 连接。进程身份在 max_tasks_per_child 回收时轮换，但亲和与串行保证不变。
    代价：某分片上的长任务会阻塞同分片其他 code，分片数可经 `STOCKDATA_WORKER_SHARDS`
    扩展（需同步增加 worker 实例）。systemd 用模板单元 `stockdata-fetcher@{0..N-1}`。
12. **已入库代码定时同步（同日新增）**：beat 任务 `sync_tracked_codes`（每交易日
    17:10）遍历 data_watermark 中的 K线/因子水位，对每个 (code, 类型) 投递
    `last_date → 今天` 的增量抓取（含 last_date 当日，覆写盘中写入的未收盘 bar）。
