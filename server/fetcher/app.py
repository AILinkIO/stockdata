"""
Celery 应用实例。

单 worker（solo pool，concurrency=1）消费任务队列：任务串行执行，
天然满足 baostock 单线程约束。beat 定时调度器单独运行。

启动 worker（本工程目录 server/ 下）:
    uv run celery -A fetcher.app worker --pool=solo --concurrency=1 --loglevel=info
"""

import logging

from celery import Celery
from celery.schedules import crontab

from settings import settings

logger = logging.getLogger(__name__)

app = Celery(
    "stockdata",
    broker=settings.broker_url,
    backend=settings.result_backend,
    include=["fetcher.tasks", "fetcher.beat"],
)

app.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    broker_transport_options={"visibility_timeout": settings.visibility_timeout},
    result_expires=600,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Shanghai",
)

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
    "sync-tracked-codes-after-close": {
        "task": "fetcher.beat.sync_tracked_codes",
        "schedule": crontab(hour=17, minute=10, day_of_week="1-5"),
    },
}
