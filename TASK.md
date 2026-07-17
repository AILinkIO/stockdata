# 任务状态

> 2026-07-17：**v2 全面重构完成** —— dotnet/MCP/Redis/独立 fetch 微服务全部下线，
> 重写为纯 Python 单服务（NiceGUI Web + 唯一 baostock 同步 worker + Typer CLI 薄客户端）。
> 架构与运维手册见 `README.md`。旧系统（dotnet 属主 + fetch 微服务）见 git 历史。

## 已完成（2026-07-17 重构）

- ✅ P0 uv 单项目脚手架（python 3.13，仓库根 `pyproject.toml`）
- ✅ P1 BaostockProvider 类化移植（限流/错误分类/relogin/熔断/watchdog/空闲登出）
  + SessionGuard：**≥5 分钟登录间隔红线 PG 持久化强制**
- ✅ P2 全新 schema（`stockdata db init|reset`，幂等 DDL，无 alembic）
- ✅ P3 同步引擎：每 (code,dataset) 水位断点续传、切片事务原子提交、
  结算边界语义、拉黑持久熔断、退避重试、advisory lock
- ✅ P4 SyncRunner 常驻线程 + `/api/sync/*` + CLI（rich TUI / plain）
- ✅ P5 NiceGUI 三页：关注列表 / K 线（ECharts，5/30/d/w + 读时复权）/ 同步仪表盘
- ✅ P6 单镜像单服务 compose（host 网络，Redis 下线）
- ✅ P7 删除 dotnet-mcp/、server/、scripts/、docs/ 旧文档；生产库推倒重建

测试：41 通过（provider/限流/守卫/复权单测零网络；引擎/API 集成测试走
stockdata_e2e 库 + FakeProvider）。

## 待办 / 后续可选

- ⬜ 首次全量同步（建议先 `--watchlist-only` 或分夜跑全市场，预期见 README 表格）
- ⬜ 宿主机 cron 接入每日增量（`stockdata sync run --plain`，重复启动由 409 天然排他）
- ⬜ 观察 baostock 分钟线切片尺寸（当前 180 自然日/片，如遇截断调小 `MINUTE_SLICE_DAYS`）

## 头号约束 ⚠️（不变）

**baostock 按 IP 限流，过频触发拉黑（10001011）。**
90/min 限流、单进程单线程、≥5 分钟登录间隔、熔断暂停均已由代码强制（README「红线」表）。
真拉黑只认 10001011；10002007 是长连接断开，relogin 自愈。
