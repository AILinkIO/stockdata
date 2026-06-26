# stockdata

A 股市场数据服务。架构：**dotnet 为唯一数据属主**（PostgreSQL + 全部 MCP 工具 serving），
**Python 为无状态 baostock 抓取微服务**。

## 架构

```
MCP 客户端 ──MCP 协议──> mcp (dotnet, :8000) ──┬── 直读 / 落盘 ──> PostgreSQL
                                              └── HTTP ──> fetch (Python, :8090) ──> baostock
```

- **mcp（[dotnet-mcp/](dotnet-mcp/)）** — C# .NET 10，唯一 PG 属主。读纯走 PG（懒登记 + 定向高优先
  有界抓取）、后台常驻 `SyncDrainer` 全量续传、落盘、以及全部工具 serving（行情/复权/财报/宏观/快照/
  交易日历/日期派生/TA-Lib 指标）。EF Core 管 schema。:8000 Streamable HTTP（MCP 协议）。
- **fetch（[server/](server/)）** — 无状态 baostock 抓取微服务（FastAPI）。只做「给定参数 → 调 baostock →
  返回数据」，**不碰 PG**。submit+poll + high/low 优先队列 + 限流 + 退避重试 + 长驻 baostock 会话。:8090。
- **PostgreSQL / Redis(Valkey)** — 物理机服务。PG 由 dotnet 独占；Redis 供 fetch 的 job 存储与限流。

## 数据流（方案 A：唯一串行驱动 baostock 的常驻 Drainer）

baostock 按 IP 限频，过频拉黑，因此**全局只有一条串行抓取通道**：dotnet 的 `SyncDrainer`
后台 worker 串行下达 job，fetch 侧单 worker 串行消费。读与后台同步靠 **high/low 优先级**错开。

**读路径（`ServeFromPgOnly=true`）** — MCP 工具读到一个 code：

1. **懒登记** — 把未纳管的 code 写入 `synced_stock` 并建一条 pending 的 `stock_sync_task`（不触 baostock）。
2. **定向高优先有界抓取** — 用 `high` 优先在 fetch 队列插队，按 `ReadFetchBudgetSeconds`（默认 30s）有界等待：
   预算内抓到 → 返回新鲜数据；超预算/失败 → **吞掉异常，回退读 PG 现状**（不挂死、不报错）。
3. 缺口由后台 Drainer 续补。

**后台同步（low 优先）** — 常驻 `SyncDrainer`：

- 从 `stock_sync_task` 取 due（pending/partial）任务，逐票 `low` 优先全量同步
  （stock_basic→k_d→adjust→dividend→financial→performance），每步幂等 + 落 `datasets_done` 步级断点 → 续传。
- 按 `MarketRefreshSeconds`（默认 1h）自维护市场级数据（交易日历→证券列表→行业→指数成分）。
- baostock 拉黑（halt）时暂停提交，由 `FetchHaltMonitor` 冷却后自动 `/restart` 恢复。

**控制面（cron 调，秒回、不抓 baostock）**：

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/sync/refresh` | 把完成早于 `StaleAfterHours`（默认 20h）的 done 任务重置 pending，交 Drainer 后台消费 |
| `GET`  | `/sync/status`  | 同步进度观测（已纳管票数 + 按状态计数） |

> 管线关闭（`PipelineEnabled=false`）时 `/sync/*` 返回 503。

## fetch 微服务

异步 submit + poll 的抓取代理（Python 不持有数据库）：

1. dotnet `POST /fetch {type, params, priority}` → 立即返回 `202 {job_id}`（**不阻塞**）。
2. fetch **单 worker 串行**消费：先 `high` 后 `low`，限流闸（每分钟 N 次）→ 调 baostock → 结果写 Redis（带 TTL）。
3. dotnet 轮询 `GET /fetch/{job_id}` 至 `done`，取回原始 payload，**自己解析并落盘到 PG**。

要点：

- **不碰 PG** — 抓到的数据经 Redis 回传给 dotnet，落盘由 dotnet 负责。
- **去重搭车** — 同参数（params_hash）的并发请求复用同一 job，只抓一次。
- **限流防拉黑** — 每分钟查询数上限（Redis 滑动窗口）+ 串行消费，保护 baostock IP。
- **halt/restart** — 拉黑（10001011/10002007）时 worker 写持久标志停摆；`GET /status` 感知、`POST /restart` 恢复（进程不退）。
- **长驻会话** — baostock 惰性登录、长连接复用；⚠️ **进程勿频繁重启**（每次重启 = 一次新登录，
  间隔须 > 5 分钟，否则可能被判异常登录频次而拉黑）。

抓取类型与 job/Redis 细节见 [server/README.md](server/README.md)。

## 部署

物理机需 PostgreSQL + Valkey(Redis)。容器走 host 网络直连本机服务。

```bash
./up.sh            # 构建并拉起 fetch + mcp（= sudo docker compose up -d --build）
./up.sh mcp        # 只拉起单个服务
./down.sh          # 停止（保留数据卷；./down.sh -v 连数据卷一起清）
sudo docker compose ps
```

配置见 [server/.env](server/.env.example)：fetch 与 mcp **共用同一份 `.env`**——mcp 用
`STOCKDATA_PG_DSN` 连 PG，fetch 用限流/重试/Redis 各项。新抓取模型的开关在 compose 的 mcp 环境段
（`PipelineEnabled` / `ServeFromPgOnly` / `FetchBase`）。

## 项目结构

| 目录 | 说明 |
|---|---|
| [server/](server/) | 无状态 baostock 抓取微服务（fetch_service，FastAPI :8090） |
| [dotnet-mcp/](dotnet-mcp/) | MCP 服务 + 唯一 PG 属主 + 常驻 Drainer（C# .NET 10，:8000 Streamable HTTP） |

## 迁移历史

本仓库从「Python FastAPI + Celery + PG 单体」迁移到「dotnet 属主 + Python 抓取微服务」，
全过程（设计决策、分阶段实施、部署验证）见 [TASK.md](TASK.md) 与 [docs/migration-k_d-e2e.md](docs/migration-k_d-e2e.md)。
