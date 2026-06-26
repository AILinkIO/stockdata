# fetch_service — 无状态 baostock 抓取微服务

只做一件事：**收到「抓什么」的请求 → 调 [Baostock](http://baostock.com/) → 把原始数据返回**。
**不碰数据库**——数据落盘与全部 serving 由 dotnet（[../dotnet-mcp/](../dotnet-mcp/)）负责。

异步 submit + poll 模型：调用方提交 job、轮询取结果；内部单 worker 串行消费，限流防 IP 拉黑。

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/fetch` | 提交抓取 job，立即返回 `202 {job_id, status, dedup}`（不阻塞） |
| `GET` | `/fetch/{job_id}` | 查询：`{job_id, status, payload?, error?}`，`status ∈ {pending,running,done,failed}` |
| `GET` | `/healthz` | 健康检查（不触 baostock） |

**提交示例**

```jsonc
POST /fetch
{ "type": "fetch_kline",
  "params": { "code": "sh.600000", "start_date": "2024-01-01",
              "end_date": "2024-03-31", "frequency": "d" } }
→ 202 { "job_id": "...", "status": "pending", "dedup": false }
```

**结果**（`done` 时）：`payload` 为 baostock 原始返回（全字符串，dotnet 侧解析）

```jsonc
{ "job_id": "...", "status": "done",
  "payload": { "fields": ["date","code","open", ...],
               "rows": [["2024-01-02","sh.600000","10.20", ...], ...] } }
```

### 支持的抓取类型（`type`）

`fetch_kline`（日/周/月/分钟，frequency 区分）、`fetch_adjust_factor`（复权因子，恒全量）、
`fetch_dividend`、`fetch_trade_calendar`、`fetch_stock_basic`、`fetch_stock_list`、
`fetch_industry`、`fetch_index_constituent`、`fetch_macro`（存贷款利率/准备金率/货币供应）、
`fetch_performance`（业绩快报/预告）、`fetch_financial_report`（六类季报，payload 为 `[report_type, record]`）。

## 工作机制

- **单 worker 串行** — 满足 baostock 单连接约束；job 从 Redis 队列取出逐个执行。
- **去重搭车** — 同 `(type, params)` 的 `params_hash` 抢占去重索引，并发请求复用同一 job、只抓一次。
- **长驻 baostock 会话** — 惰性登录（首次抓取才 `bs.login`），长连接复用，出错才重连。
- **限流防拉黑 / 退避重试 / Redis job 存储** — 详见下方「配置」节的「限流机制」「退避重试」「Redis 布局」三小节。

> ⚠️ **进程长驻、勿频繁重启**：每次重启 = 一次新 `bs.login`，间隔须 **> 5 分钟**，
> 否则可能被 baostock 判异常登录频次而拉黑。健康检查/滚动发布/crash-loop 退避都要满足这条下限。

## 配置

环境变量前缀 `STOCKDATA_`，可写入本目录 `.env`（见 [.env.example](.env.example)）。
fetch 与 mcp **共用同一份 `.env`**——`PG_DSN` 只有 mcp 读，其余只有 fetch 读。

| 变量 | 默认 | 读取方 | 说明 |
|---|---|---|---|
| `PG_DSN` | — | **mcp** | dotnet 连 PG 的 DSN（`postgresql+psycopg://…`）。**fetch 不读**（Python 不碰 PG） |
| `FETCH_RATE_LIMIT_PER_MINUTE` | `90` | fetch | 每分钟最多向 baostock 发起的查询次数；**`<=0` 关闭限流** |
| `RATE_LIMIT_BACKEND` | `redis` | fetch | 限流实现：`memory`（进程内）/ `redis`（跨进程共享） |
| `RATE_LIMIT_REDIS_URL` | `redis://127.0.0.1:6379/1` | fetch | `backend=redis` 时的 Redis 地址（建议独立 DB） |
| `FETCH_JOB_REDIS_URL` | `redis://127.0.0.1:6379/2` | fetch | job 状态/去重/结果存储（独立 DB，避免与限流冲突） |
| `BAOSTOCK_SOCKET_TIMEOUT` | `30` | fetch | baostock TCP 超时（秒）：挂死时靠它快速失败重连 |
| `FETCH_MAX_RETRIES` | `8` | fetch | `DataSourceError` 退避重试次数上限，耗尽才标 `failed` |
| `FETCH_RETRY_BASE_SECONDS` | `30` | fetch | 退避基数（见下「退避重试」） |
| `FETCH_RETRY_MAX_BACKOFF_SECONDS` | `180` | fetch | 单次退避等待封顶 |

> `.env` 由 compose 的 `env_file` 在**容器启动时**注入；改了 `.env` 需重启容器才生效
> （fetch 重启 = 一次新 `bs.login`，注意 >5min 间隔）。

### 限流机制（防 IP 拉黑）

baostock 按 IP 限制查询频率，过频会被拉黑。两道防线：**单 worker 串行** + **滑动窗口限流**。
provider 在**每次** baostock 查询前 `acquire()` 一个额度（`core/ratelimit.py`）：

- **滑动窗口语义** — 任意 `period`（60s）内最多放行 `max_calls`（默认 60）次；窗口内允许突发到上限，
  之后阻塞，直到最早的一次调用滑出窗口腾出额度。不是「整秒重置」的固定窗口，更平滑。
- **`memory` 后端** — 进程内 `deque` 记录每次调用时间戳 + `threading.Lock`，零外部依赖，**适合单实例**。
- **`redis` 后端** — 所有实例共享一个 Redis ZSET key（`ratelimit:baostock`），用 **Lua 脚本原子**完成
  「清过期成员 → 计数 → 未满则入队 / 已满则返回需等待毫秒」。**多实例/多进程安全**，本项目默认用它
  （即便单 worker，也便于将来横向扩展或与其它进程共享额度）。
- `acquire()` 额度耗尽时**阻塞等待**（memory: sleep 到最早调用滑出；redis: sleep 返回的 wait_ms 后重试），
  因此抓取整体不会超过设定速率，单 worker 串行天然不并发打 baostock。
- `FETCH_RATE_LIMIT_PER_MINUTE <= 0` 时所有方法直接通过（关闭限流，仅测试用）。

### 退避重试

worker 对可重试的 `DataSourceError`（连接断开/超时等）做**指数退避**：
第 n 次失败后等待 `base × 2ⁿ` 秒、封顶 `max_backoff`，默认 `30 → 60 → 120 → 180 → 180 …`；
重试到 `FETCH_MAX_RETRIES` 次仍失败才标 `failed`。`NoDataFoundError`（停牌/未发布）是**合法空结果**，
直接 `done` 空 payload，由 dotnet 按水位规则处理，不算失败。

### Redis 布局

| DB | 用途 | key |
|---|---|---|
| db1 | 限流滑动窗口 | `ratelimit:baostock`（ZSET） |
| db2 | job 存储 | `job:{id}`(hash 状态) / `job:result:{id}`(payload) / `job:idx:{params_hash}`(去重索引) / `fetch:pending`(待消费队列 list) |

TTL：完成态 job + payload + 去重索引 600s；在途 job 心跳续期至 1200s（僵尸阈值），超时则释放去重、由调用方重建。

## 运行

```bash
# 本地（需 Redis 在跑）
uv run --no-dev uvicorn fetch_service.app:app --host 0.0.0.0 --port 8090

# 容器（仓库根目录）
../up.sh           # 构建并拉起 fetch + mcp
```

## 代码结构

```
fetch_service/        # HTTP 入口(app) + Redis job 存储(jobs) + 串行 worker(worker)
fetcher/providers/    # baostock provider（query_* + 限流 + 登录/重连）
core/ratelimit.py     # 滑动窗口限流（memory / redis 两实现）
settings.py           # 配置（pydantic-settings）
```
