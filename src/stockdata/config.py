"""全局配置（pydantic-settings，环境变量前缀 STOCKDATA_，支持根目录 .env）。"""

from datetime import date

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="STOCKDATA_", env_file=".env", extra="ignore"
    )

    # ── PostgreSQL ──
    pg_dsn: str = "postgresql://stockdata@127.0.0.1:5432/stockdata"

    # ── Web / API ──
    web_host: str = "0.0.0.0"
    web_port: int = 8050
    app_base: str = "http://127.0.0.1:8050"  # CLI 客户端连接的服务地址
    api_key: str = ""  # /api/v1 数据面鉴权：空=关闭；配置后要求 X-API-Key 头

    # ── 崩溃恢复：启动时把遗留 running 任务收尾成 interrupted 并自动续跑 ──
    resume_interrupted_on_start: bool = True

    # ── baostock ──
    baostock_socket_timeout: int = 30  # TCP 超时：挂死靠它快速失败重连

    # ── 限流（防 IP 拉黑）──
    rate_limit_per_minute: int = 90  # 每分钟最多向 baostock 发起的查询次数，<=0 关闭

    # ── 登录间隔红线：任何两次 bs.login() 间隔必须 ≥ 此秒数（PG 持久化，跨进程重启生效）──
    min_login_interval_seconds: int = 300

    # ── 抓取退避重试（DataSourceError，引擎统一处理）──
    max_retries: int = 8
    retry_base_seconds: int = 30          # 指数退避基数（30→60→120→…）
    retry_max_backoff_seconds: int = 180  # 单次退避等待封顶

    # ── watchdog 硬超时（兜底 baostock 库内部 recv 死循环）──
    watchdog_timeout_seconds: int = 600

    # ── 空闲自动登出（防复用服务端已断的僵死 socket 报 10002007）──
    idle_logout_seconds: int = 900  # <=0 关闭

    # ── 网络接收错误(10002007)熔断：连续达此次数升级为 halt ──
    receive_error_halt_threshold: int = 5

    # ── 同步回填边界与节奏 ──
    minute_backfill_floor: date = date(2023, 1, 1)     # 分钟线最早回填日
    financial_backfill_floor: date = date(2020, 1, 1)  # 财报默认回填下限（全历史成本过高）
    kline_slice_days: int = 3650   # 日/周线切片跨度（自然日）
    minute_slice_days: int = 180   # 分钟线切片跨度（自然日，5 分线 ≈6k 行/片）
    stale_after_hours: int = 20    # 未结算数据集（分红/财报/快报等）的重查间隔
    snapshot_refresh_days: int = 7  # 快照类（行业/指数成分/复权因子兜底）的重抓间隔

    @property
    def pg_conninfo(self) -> str:
        """psycopg3 连接串：兼容旧 sqlalchemy 风格 postgresql+psycopg:// 前缀。"""
        return self.pg_dsn.replace("postgresql+psycopg://", "postgresql://", 1)


settings = Settings()
