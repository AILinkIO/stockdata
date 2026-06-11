# 迁移实施计划：MCP 服务 → Web 服务 + Celery 任务体系

> 状态：全部阶段已完成（0~7）。新旧对照 25/25 通过
> 设计依据：[refactor-design.md](refactor-design.md)（架构、技术选型、表结构均以设计文档为准，本文档只描述实施步骤）

## 总览

| 阶段 | 内容 | 工期 | 依赖 |
|---|---|---|---|
| 0 | 基础设施与可行性验证 | 0.5 天 | — |
| 1 | 数据模型与 ORM | 1~2 天 | 0 |
| 2 | fetcher 任务系统 | 2~3 天 | 1 |
| 3 | 覆盖度/新鲜度服务 | 1~2 天 | 1（可与 2 并行） |
| 4 | FastAPI 服务 | 2~3 天 | 2、3 |
| 5 | 定时同步 | 0.5~1 天 | 2 |
| 6 | 对照验证与切换 | 1~2 天 | 4、5 |
| 7（可选） | MCP 薄壳 | 0.5 天 | 6 |

总计 **8~13 个工作日**。关键路径：阶段 0 的兼容性验证 → 阶段 2/3。
每个阶段独立验收，验收不通过不进入下一阶段。

原则：迁移期间 `main` 分支始终可运行。新代码在新目录（`api/`、`fetcher/`、`db/`）中
生长，旧 MCP 服务（`main.py` + `src/`）保持可用，直到阶段 6 对照通过后一次性删除。

---

## 阶段 0：基础设施与可行性验证（0.5 天）

### 任务

1. 基础设施确认（实施时实测调整）：PG 17.9 已运行；Redis 角色由系统已有的
   **Valkey 8** 承担（Debian 13 以 Valkey 替代 Redis，协议兼容，Celery 直接可用，
   不另起 redis-server 抢端口）。该实例与其他应用共享：db0 已被占用，本项目约定
   **db2 = Celery broker、db3 = 结果后端**；**不开启 AOF**（共享实例不动其持久化
   配置；任务可重建，丢队列可接受，见设计文档 3 章选型理由）。
   PG 初始化：建库 `stockdata` 与专用账号。
2. `uv add`：`celery[redis]`、`fastapi`、`uvicorn`、`sqlalchemy`、`alembic`、
   `psycopg[binary]`、`pydantic-settings`。
3. 新建 `settings.py`（pydantic-settings）：PG DSN、Redis URL、fetcher 参数
   （并发数、max_tasks_per_child、超时、visibility_timeout）、分钟线回填起点。
4. **冒烟测试（本阶段核心）**：Python 3.14 下起一个最小 Celery prefork worker，
   验证：
   - 任务正常执行与结果返回；
   - `task_time_limit` 触发时子进程被 SIGKILL 且 worker 自动补充新子进程；
   - `worker_max_tasks_per_child` 到数后子进程被回收重建；
   - `worker_process_init` 信号在子进程内触发（用它跑一次 `bs.login()`）。

### 风险分支

冒烟测试不通过（billiard/celery 对 3.14 不兼容）时，按设计文档 9.1 决策：

- 路线 A：`requires-python` 放宽到 3.13，重跑冒烟；
- 路线 B：放弃 Celery，改手写 Redis 队列消费（`BRPOP`）+ `multiprocessing(spawn)`
  子进程池，自实现超时 kill 与回收（阶段 2 工期 +2 天）。

### 验收

PG/Redis 服务健康（`systemctl status` 正常、psql 可连、`redis-cli ping` 返回 PONG）；
冒烟测试四项全过（或已明确降级路线）。

---

## 阶段 1：数据模型与 ORM（1~2 天）

### 任务

1. `db/session.py`：双引擎——API 用 async engine（psycopg async），
   fetcher 任务内用 sync engine（psycopg sync）。
2. `db/models/` 按设计文档第 5 章建模，分模块：
   - `kline.py`：`kline`、`kline_minute`（分区表）
   - `adjust.py`：`adjust_factor`、`dividend`
   - `financial.py`：`financial_report`
   - `market.py`：`stock_basic`、`trade_calendar`、`stock_list_snapshot`、
     `index_constituent`、`stock_industry`
   - `macro.py`：宏观 5 表
   - `meta.py`：`data_watermark`、`fetch_task`
3. Alembic 初始化 + 首个 migration。注意：
   - `kline_minute` 的 RANGE 分区与预建分区（建议预建至次年）需手写 op.execute，
     autogenerate 不支持；
   - `fetch_task` 的部分唯一索引（`WHERE status IN ('pending','running')`）同样手写。
4. `data_type` / `report_type` / `task_type` 枚举定义在 Python 层
   （`db/models/enums.py`），库中存 varchar。

### 验收

- 空库执行 `alembic upgrade head` 建出全部表、分区、索引；
- `alembic downgrade base` 可干净回滚；
- 模型与 DDL 一致性 review（重点：主键、NUMERIC 精度、timestamptz）。

---

## 阶段 2：fetcher 任务系统（2~3 天，核心阶段）

### 任务

1. `fetcher/app.py`：Celery 实例 + 设计文档 4.1 节配置；
   `worker_process_init` 中登录 baostock（迁移现 `context.py` 的
   `_do_login`/`_suppress_stdout`/退避重试逻辑，但**不再有线程与队列**）。
2. `fetcher/providers/`：迁入 `src/providers/interface.py` 与 `baostock.py`。
   改动点：
   - 删除对 `context.execute()` 的依赖——子进程内直接调用 bs API
     （进程隔离取代线程串行化）；
   - 保留 `_check_api_error` / `_collect_rows` / 重试错误码判定
     （`_RETRYABLE_CODES`），可重试错误在任务内重登录一次，仍失败则抛出，
     交给 Celery `autoretry_for` + 退避重试。
3. `fetcher/writer.py`：DataFrame → 类型解析（字符串 → NUMERIC/date/bool）→
   `INSERT ... ON CONFLICT DO UPDATE` 批量 upsert → 同事务内更新 `data_watermark`。
   **落库与水位更新必须同事务**，否则 kill 时序产生"有数据无水位"或反之。
4. `fetcher/tasks.py`：设计文档 4.2 节的 11 个任务。统一骨架：
   标记 `fetch_task.running` → provider 查询 → writer 落库 →
   标记 `succeeded`/`failed`。
5. 首次触达回填策略：`fetch_kline` 收到的任务参数由 API 侧计算好缺口区间；
   provider 层只管"给定区间抓全"。

### 验收（三条链路）

| 链路 | 操作 | 预期 |
|---|---|---|
| 正常 | 手工投递 `fetch_kline(sh.600000, 2024 全年, d)` | kline 表落库、watermark 正确、fetch_task=succeeded |
| 超时挂死 | 投递人为 `sleep(120)` 的测试任务 | 90s 被 SIGKILL，worker 补充新子进程，任务重试或标记 failed |
| 进程回收 | max_tasks_per_child=3 连投 10 个任务 | 观察到子进程 PID 每 3 个任务轮换，结果全部正确 |

另：删除 `src/providers/context.py` 在本阶段**不执行**（旧 MCP 服务仍依赖），
留到阶段 6。

---

## 阶段 3：覆盖度/新鲜度服务（1~2 天，可与阶段 2 并行）

### 任务

1. `db/coverage.py`：实现设计文档 5.4 节规则表。对外两个函数：
   - `check(code, data_type, start, end) -> Fresh | Gap(missing_range) | Stale`
   - `plan_fetch(...) -> 任务参数`（含首次触达的全史回填区间计算）
2. 特殊规则单测覆盖：
   - 周/月线"本周一/本月 1 日"定型边界；
   - 季报披露截止日四个分支（含 Q4 跨年）；
   - 宏观 2 个月沉淀期；
   - 全市场数据集（code=''）路径。

### 验收

规则单元测试全绿；用例覆盖 5.4 节表格的每一行至少一个正例一个反例。

---

## 阶段 4：FastAPI 服务（2~3 天）

### 任务

1. `api/main.py`：应用工厂、lifespan（engine 创建/销毁）、统一异常处理器
   （设计文档 6.3 节映射）。
2. `api/schemas/`：Pydantic 请求/响应模型。请求校验吸收
   `src/services/validation.py` 的规则（code 格式、日期格式、频率/复权枚举）。
3. `api/services/`：
   - 读穿透编排：coverage.check → 缺口则写 fetch_task + 投递 → AsyncResult
     轮询等待（asyncio.sleep 0.5s，总超时 60s）→ 读库；
   - 复权计算：kline JOIN adjust_factor，应用层按 baostock 口径乘算前/后复权；
   - 财报合并：六类 financial_report 行合并为综合指标（取代原
     `get_fina_indicator` 组装逻辑）。
4. `api/routers/`：8 个路由模块（设计文档 6.1 节映射表）+ 批量回填异步接口
   （`POST /api/v1/tasks/backfill`、`GET /api/v1/tasks/{id}`）。
5. `core/` 的分析逻辑（`core/analysis.py`）改为从 PG 读数（经 services 层），
   `formatting/markdown.py` 挂到 analysis 端点的 `Accept: text/markdown` 分支。

### 验收

- OpenAPI 文档完整可浏览（/docs）；
- 端到端冒烟：**空库**启动 → 请求 `GET /api/v1/stocks/sh.600000/kline?...` →
  自动触发抓取 → 返回数据；二次请求直接命中库（响应时间显著下降）；
- 复权口径抽查：同参数下新接口前复权价 vs 旧 MCP 工具输出一致。

---

## 阶段 5：定时同步（0.5~1 天）

### 任务

1. `fetcher/beat.py`：设计文档 4.4 节调度表（交易日历 08:00、股票列表与成分股
   每交易日 17:00/17:30）。
2. "每交易日"判定：任务开头查 `trade_calendar`，非交易日直接 skip。
3. `deploy/` 增加 `stockdata-beat.service` systemd 单元（连同本阶段一并补齐
   `stockdata-api.service`、`stockdata-fetcher.service`，均 `Requires=`
   postgresql/redis 服务）。

### 验收

手动把调度时间改到最近时刻，观察任务按时投递、落库、watermark 更新。

---

## 阶段 6：对照验证与切换（1~2 天）

### 任务

1. 对照脚本 `scripts/parity_check.py`：抽样 20~30 组查询参数（覆盖全部 8 个
   数据域、复权与不复权、历史与含今日区间），同时调用旧 MCP 工具函数与新 REST
   接口，比对**数据内容**（数值逐项比对，忽略格式差异）。
2. 对照通过后：
   - 打 tag `pre-restructure`；
   - 删除 `main.py`、`src/server.py`、`src/tools/`、`src/services/`、
     `src/providers/`（含 context.py、cache.py）、`src/data_source.py`；
   - `uv remove fastmcp diskcache`；
   - `docs/cache-strategy.md` 标注"已被 refactor-design.md 5.4 节取代"并归档；
   - 重写 README（启动方式改为 systemd 单元 / 开发期 `uv run`，
     接口文档指向 /docs）。
3. 清理 `.cache/stockdata/` 的 gitignore 条目（diskcache 目录不再产生）。

### 验收

- 对照脚本零差异（或差异均有明确解释且确认为旧实现缺陷）；
- 全仓 `grep -r "fastmcp\|diskcache\|import mcp"` 无残留；
- 新服务从零启动跑通端到端冒烟。

---

## 阶段 7（可选）：MCP 薄壳（0.5 天）

如仍需 LLM 直接接入：新建独立小项目（或 `mcp_shim/` 目录），FastMCP 工具
一一映射 REST 端点，纯 HTTP 转发，无业务逻辑。与主体架构完全解耦，
后续 REST 接口演进只需同步薄壳的参数签名。

---

## 回滚预案

- 阶段 0~5 期间：旧 MCP 服务未动，随时可用，无回滚成本；
- 阶段 6 之后：`git checkout pre-restructure` 即回到完整旧实现；
  本机的 PG/Redis 服务与旧实现无耦合，留存不影响回滚。
