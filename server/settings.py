"""
fetch_service 配置（pydantic-settings）。

仅保留无状态 baostock 抓取微服务所需项。所有配置可用环境变量覆盖（前缀 STOCKDATA_），
亦可写入本目录 .env。数据落盘/PG 由 dotnet 负责，Python 不碰 PG。
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="STOCKDATA_", env_file=".env", extra="ignore"
    )

    # ── baostock ──
    baostock_socket_timeout: int = 30  # baostock TCP 超时：挂死靠它快速失败重连

    # ── 限流（防 IP 拉黑）──
    fetch_rate_limit_per_minute: int = 60   # 每分钟最多向 baostock 发起的查询次数，<=0 关闭
    rate_limit_backend: str = "redis"        # memory（进程内）或 redis（跨进程）
    rate_limit_redis_url: str = "redis://127.0.0.1:6379/1"  # 限流 Redis（独立 DB）

    # ── 抓取退避重试（DataSourceError，worker 统一处理）──
    fetch_max_retries: int = 8          # 退避重试次数上限，耗尽才标记 failed
    fetch_retry_base_seconds: int = 30  # 指数退避基数（30→60→120→…）
    fetch_retry_max_backoff_seconds: int = 180  # 单次退避等待封顶（默认 3 分钟）

    # ── watchdog 硬超时（兜底 baostock 库内部死循环）──
    # baostock socketutil.send_msg 在服务端断连后 recv→b"" 无限自旋，
    # socket 超时对此无效（recv 不阻塞）。watchdog 到点强制注入异常中断。
    fetch_watchdog_timeout_seconds: int = 600  # 单次 baostock 查询的硬超时（默认 10 分钟）

    # ── 空闲自动登出（防服务端断连后复用僵死 socket 报 10002007）──
    # worker 闲置超过此秒数即主动 bs.logout 关连接，下次查询重新 login。<=0 关闭。
    fetch_idle_logout_seconds: int = 900  # 默认 15 分钟

    # ── 网络接收错误(10002007)熔断 ──
    # 10002007 默认按可重试处理（relogin 自愈）；连续达到此次数仍失败才升级为
    # 暂停（BlacklistError → 待 /restart），避免把瞬时网络抖动误判为 IP 拉黑。
    fetch_receive_error_halt_threshold: int = 5


settings = Settings()
