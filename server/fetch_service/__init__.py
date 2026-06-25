"""
无状态 baostock 抓取微服务（迁移目标形态，见仓库 TASK.md / docs/migration-k_d-e2e.md）。

对外 HTTP：POST /fetch（异步提交，202+job_id）、GET /fetch/{id}（查状态/取 payload）。
job 状态/去重/结果存 Redis（不依赖 PG）。内部单 worker 串行消费，复用 providers.baostock
的限流（redis db4）+ 登录会话 + 退避重试。**进程长驻、单例 baostock 会话、重启间隔 > 5 分钟**
（每次重启 = 新 bs.login，太频繁会被拉黑，见 TASK §0 红线）。
"""
