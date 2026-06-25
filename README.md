# stockdata

A 股市场数据服务。架构：**dotnet 为唯一数据属主**（PostgreSQL + 全部 MCP 工具 serving），
**Python 为无状态 baostock 抓取微服务**。

## 架构

```
MCP 客户端 ──MCP 协议──> mcp (dotnet, :8000) ──┬── 直读 / 落盘 ──> PostgreSQL
                                              └── HTTP ──> fetch (Python, :8090) ──> baostock
```

- **mcp（[dotnet-mcp/](dotnet-mcp/)）** — C# .NET 10，唯一 PG 属主。读穿透判定（coverage）、抓取编排、
  落盘、以及全部工具 serving（行情/复权/财报/宏观/快照/交易日历/日期派生/TA-Lib 指标/分析报告）。
  EF Core 管 schema。:8000 Streamable HTTP（MCP 协议）。
- **fetch（[server/](server/)）** — 无状态 baostock 抓取微服务（FastAPI）。只做「给定参数 → 调 baostock →
  返回数据」，**不碰 PG**。限流 + 退避重试 + 长驻 baostock 会话。:8090。
- **PostgreSQL / Redis(Valkey)** — 物理机服务。PG 由 dotnet 独占；Redis 供 fetch 的 job 存储与限流。

## 新 fetch 微服务如何工作

fetch 是一个**异步 submit + poll** 的抓取代理（Python 不再持有数据库）：

1. dotnet 判定需要某段数据 → `POST /fetch {type, params}` → fetch 立即返回 `202 {job_id}`（**不阻塞**）。
2. fetch 内部**单 worker 串行**消费 job：限流闸（每分钟 N 次，防 IP 拉黑）→ 调 baostock →
   结果写入 Redis（job 状态 + 原始 payload，带 TTL）。
3. dotnet 轮询 `GET /fetch/{job_id}` 至 `done`，取回原始 payload，**自己解析并落盘到 PG**。

要点：

- **不碰 PG** — 抓到的数据经 Redis 回传给 dotnet，落盘由 dotnet 负责。
- **去重搭车** — 同参数（params_hash）的并发请求复用同一 job，只抓一次。
- **限流防拉黑** — 每分钟查询数上限（Redis 滑动窗口）+ 串行消费，保护 baostock IP。
- **长驻会话** — baostock 惰性登录、长连接复用；⚠️ **进程勿频繁重启**（每次重启 = 一次新登录，
  间隔须 > 5 分钟，否则可能被判异常登录频次而拉黑）。

抓取类型与 job/Redis 细节见 [server/README.md](server/README.md)。

## 部署

物理机需 PostgreSQL + Valkey(Redis)。容器走 host 网络直连本机服务。

```bash
./up.sh            # 构建并拉起 fetch + mcp（= sudo docker compose up -d --build）
./down.sh          # 停止（保留数据卷）
sudo docker compose ps
```

配置见 [server/.env](server/.env.example)：mcp 用 `STOCKDATA_PG_DSN` 连 PG，fetch 用限流/重试/Redis 各项。

## 项目结构

| 目录 | 说明 |
|---|---|
| [server/](server/) | 无状态 baostock 抓取微服务（fetch_service，FastAPI :8090） |
| [dotnet-mcp/](dotnet-mcp/) | MCP 服务 + 唯一 PG 属主（C# .NET 10，:8000 Streamable HTTP） |

## 迁移历史

本仓库从「Python FastAPI + Celery + PG 单体」迁移到「dotnet 属主 + Python 抓取微服务」，
全过程（设计决策、分阶段实施、部署验证）见 [TASK.md](TASK.md) 与 [docs/migration-k_d-e2e.md](docs/migration-k_d-e2e.md)。
