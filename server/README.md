# stockdata

中国 A 股市场数据服务。数据源为 [Baostock](http://baostock.com/)，
PostgreSQL 做数据仓库，Celery（Redis broker）+ 多进程任务体系负责抓取，
FastAPI 对外提供 REST 接口。

文档：[API 参考](docs/api.md) · [数据生命周期（读穿透/缺口补抓/复权）](docs/data-lifecycle.md) · [架构设计](docs/refactor-design.md)

## 功能

- **股票行情** — K 线（日/周/月/分钟级）、基本信息、分红送转、复权因子（前/后复权读时计算）
- **财务报表** — 盈利/营运/成长/偿债/现金流/杜邦六类季报、业绩快报/预告、综合财务指标
- **指数与行业** — 上证 50、沪深 300、中证 500 成分股，行业分类
- **市场概览** — 交易日历、全部股票列表
- **宏观经济** — 存贷款利率、存款准备金率、货币供应量
- **日期工具** — 最新交易日、交易日判断、前后交易日
- **分析报告** — 个股基本面 / 技术面 / 综合分析（Markdown）

数据按需抓取（读穿透：缺数据时自动投递抓取任务并等待），辅以每日定时同步：
交易日历、股票列表、指数成分股、行业分类，以及**已入库代码的交易信息增量同步**
（每交易日收盘后遍历水位表，对所有跟踪中的标的补抓 K 线/复权因子到当日）。

## 架构

```
HTTP ──▶ api/（FastAPI）──读──▶ PostgreSQL ◀──写── fetcher/（Celery worker）
              │ 缺数据时投递任务          ▲                 │ fork 子进程执行
              └─────▶ Redis 队列 ────────┴─────────────────┘ 超时 SIGKILL 兜底
                        ▲
              beat（定时同步调度）
```

- baostock 全局 TCP 连接的挂死问题由**进程隔离**解决：每个查询在 Celery prefork
  子进程内执行，子进程处理 N 个任务后回收，挂死时被 `task_time_limit` SIGKILL。
- 只存原始事实（不复权 K 线 + 复权因子序列），复权价读取时计算，除权事件零失效。
- 新鲜度规则（历史数据永久、盘中 5 分钟、财报披露期等）见 `db/coverage.py`。

## 快速开始

需要 Python 3.14+、PostgreSQL 17、Redis 协议服务（Redis/Valkey），
使用 [uv](https://docs.astral.sh/uv/) 管理依赖。

```bash
# 安装依赖
uv sync

# 配置（本工程目录 server/ 下的 .env）
# STOCKDATA_PG_DSN=postgresql+psycopg://stockdata:<password>@127.0.0.1:5432/stockdata
# STOCKDATA_BROKER_URL=redis://127.0.0.1:6379/2
# STOCKDATA_RESULT_BACKEND=redis://127.0.0.1:6379/3

# 初始化数据库
uv run alembic upgrade head

# 启动方式一：Docker Compose（推荐）
# PG/Valkey 用物理机服务，api/fetcher×3分片/beat 在容器（host 网络直连 127.0.0.1）；
# migrate 服务先跑 alembic upgrade head，成功后其余服务才启动
sudo docker compose up -d --build

# 启动方式二：裸机进程（开发/调试）
# 同 code 同任务类型恒定路由到同一分片（单进程），天然串行并复用连接
uv run celery -A fetcher.app worker -Q shard0 -n shard0@%h -c 1 --loglevel=info
uv run celery -A fetcher.app worker -Q shard1 -n shard1@%h -c 1 --loglevel=info
uv run celery -A fetcher.app worker -Q shard2 -n shard2@%h -c 1 --loglevel=info
uv run celery -A fetcher.app beat --loglevel=info     # 定时同步
uv run uvicorn api.main:app --host 0.0.0.0 --port 8080  # API
# 裸机常驻可用 systemd 单元（deploy/，fetcher 为模板单元 stockdata-fetcher@{0..2}）；
# 两种方式二选一，不可同时运行（端口与队列消费会冲突）
```

接口文档：<http://localhost:8080/docs>

数据获取与任务提交均通过 REST：数据端点自带读穿透（缺数据自动抓取），
批量回填走 `POST /api/v1/tasks/backfill`（202 + task_id 轮询）。

```bash
# 示例
curl "localhost:8080/api/v1/stocks/600000/kline?start_date=2024-01-01&end_date=2024-12-31"
curl "localhost:8080/api/v1/stocks/600000/kline?start_date=2024-01-01&end_date=2024-12-31&adjust_flag=2"  # 前复权
curl "localhost:8080/api/v1/stocks/600000/financials/profit?year=2024&quarter=3"
curl "localhost:8080/api/v1/stocks/600000/analysis"
curl "localhost:8080/api/v1/dates/latest-trading-day"
```

## 项目结构

```
api/                  # FastAPI 应用
├── routers/          # REST 路由（行情/财报/指数/市场/宏观/日期/工具/任务）
├── services/         # 读穿透编排、复权计算、财报合并、交易日工具
└── errors.py         # 领域异常 → HTTP 状态码
fetcher/              # Celery 任务体系
├── app.py            # Celery 实例 + 子进程生命周期配置
├── tasks.py          # 抓取任务（查询 → 解析 → upsert → 水位更新）
├── beat.py           # 定时同步任务
└── providers/        # Baostock 查询函数（子进程内执行）
db/
├── models/           # SQLAlchemy 2.0 模型（17 张表）
├── coverage.py       # 覆盖度/新鲜度规则
└── alembic/          # schema 迁移
core/                 # 代码标准化等纯逻辑
deploy/               # systemd 单元
scripts/              # smoke_celery.py（部署验证）
```

## 运维

- **冒烟验证**：`uv run python scripts/smoke_celery.py`（验证 Celery 子进程
  生命周期四项行为）；`fetcher.debug_probe` 任务可在线探测 worker。
- **分钟线分区**：`kline_minute` 按年分区，已预建至 2027 + DEFAULT 兜底，
  每年在 Alembic 中追加下一年分区。
- **任务观测**：`fetch_task` 表记录每次抓取的参数、状态与错误。
- 旧 MCP 实现保留在 git tag `pre-restructure`。
