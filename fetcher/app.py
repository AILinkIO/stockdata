"""
Celery 应用实例与子进程生命周期配置（设计文档 4.1 节）。

启动 worker（项目根目录）:
    uv run celery -A fetcher.app worker --loglevel=info
"""

import logging

from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown

from settings import settings

logger = logging.getLogger(__name__)

app = Celery(
    "stockdata",
    broker=settings.broker_url,
    backend=settings.result_backend,
    include=["fetcher.tasks", "fetcher.beat"],
)

app.conf.update(
    # 子进程生命周期
    worker_concurrency=settings.worker_concurrency,
    worker_max_tasks_per_child=settings.worker_max_tasks_per_child,
    task_time_limit=settings.task_time_limit,
    task_soft_time_limit=settings.task_soft_time_limit,
    # 可靠性：子进程被 kill 后任务重回队列
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # Redis broker 的重投递窗口，必须 > task_time_limit
    broker_transport_options={"visibility_timeout": settings.visibility_timeout},
    result_expires=settings.result_expires,
    # 序列化
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Shanghai",
)

# ── 定时同步调度表（任务实现见 fetcher/beat.py，设计文档 4.4 节） ──
from celery.schedules import crontab  # noqa: E402

app.conf.beat_schedule = {
    "sync-calendar-daily": {
        "task": "fetcher.beat.sync_calendar",
        "schedule": crontab(hour=8, minute=0),
    },
    "refresh-yesterday-list": {
        "task": "fetcher.beat.refresh_yesterday_list",
        "schedule": crontab(hour=8, minute=30),
    },
    "sync-market-after-close": {
        "task": "fetcher.beat.sync_market",
        "schedule": crontab(hour=17, minute=0, day_of_week="1-5"),
    },
}


@worker_process_init.connect
def _init_child(**kwargs):
    """子进程 fork 后登录 baostock（父进程永不接触 baostock）。

    登录失败不在此处抛出：任务执行时 ensure_login() 会再尝试并正确走
    Celery 的重试/失败路径，避免子进程无限重生循环。
    """
    from fetcher.providers import baostock as provider

    try:
        provider.ensure_login()
    except Exception as e:
        logger.warning("子进程初始登录失败，任务执行时将重试: %s", e)


@worker_process_shutdown.connect
def _shutdown_child(**kwargs):
    from fetcher.providers import baostock as provider

    provider.logout()
