"""
全局配置模块（pydantic-settings）。

所有配置项可通过环境变量覆盖，前缀 STOCKDATA_，亦可写入本工程目录（server/）的 .env 文件。
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="STOCKDATA_", env_file=".env", extra="ignore"
    )

    # ── 数据库 ──
    pg_dsn: str = "postgresql+psycopg://stockdata@127.0.0.1:5432/stockdata"

    # ── Celery（单 worker solo pool + Redis broker） ──
    broker_url: str = "redis://127.0.0.1:6379/2"
    result_backend: str = "redis://127.0.0.1:6379/3"  # 仅供调试/inspect；等待走轮询 fetch_task
    visibility_timeout: int = 600      # Redis broker 重投递窗口

    # ── baostock ──
    baostock_socket_timeout: int = 30  # baostock TCP 超时：挂死靠它快速失败重连
    fetch_rate_limit_per_minute: int = 60  # 每分钟最多向数据源发起的查询次数（防 IP 拉黑），<=0 关闭

    # ── API 读穿透 ──
    fetch_wait_timeout: int = 120      # 读穿透轮询等待抓取任务完成的超时（秒）

    # ── 数据回填 ──
    minute_backfill_start: str = "2023-01-01"  # 分钟线回填起点（全史过大）


settings = Settings()
