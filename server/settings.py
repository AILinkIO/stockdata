"""
全局配置模块（pydantic-settings）。

所有配置项可通过环境变量覆盖，前缀 STOCKDATA_，亦可写入本工程目录（server/）的 .env 文件。
详见 docs/refactor-design.md 与 docs/migration-plan.md。
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="STOCKDATA_", env_file=".env", extra="ignore"
    )

    # ── 数据库 ──
    pg_dsn: str = "postgresql+psycopg://stockdata@127.0.0.1:5432/stockdata"

    # ── 队列（共享 Valkey 实例，db0/db1 属于其他应用） ──
    broker_url: str = "redis://127.0.0.1:6379/2"
    result_backend: str = "redis://127.0.0.1:6379/3"

    # ── fetcher 子进程生命周期（见设计文档 4.1 节 / 实施偏差 11） ──
    worker_shards: int = 3             # 分片队列数：同 code 同任务类型恒定路由到同一分片
    worker_concurrency: int = 1        # 每个分片 worker 的子进程数；亲和性要求恒为 1
    worker_max_tasks_per_child: int = 20  # 处理 N 个任务后杀死回收子进程（=1 即一任务一进程）
    task_time_limit: int = 90          # 硬超时：SIGKILL 子进程
    task_soft_time_limit: int = 60     # 软超时：先抛异常给任务清理机会
    visibility_timeout: int = 600      # Redis broker 重投递窗口，必须 > task_time_limit
    result_expires: int = 600          # 结果只为读穿透等待服务

    # ── API 读穿透 ──
    fetch_wait_timeout: int = 120      # 等待抓取任务完成的总超时（秒）；
                                       # 须 ≥ task_time_limit + 余量，保证提交方能等到
                                       # 任务终态（成功/硬超时被杀），而非中途放弃后
                                       # 让重试撞上去重锁

    # ── 数据回填 ──
    minute_backfill_start: str = "2023-01-01"  # 分钟线回填起点（全史过大）


settings = Settings()
