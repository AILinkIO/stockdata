"""
Celery 应用实例与子进程生命周期配置（设计文档 4.1 节）。

队列按 crc32(任务名:code) 分片：同 code 同任务类型的任务恒定路由到同一分片，
每个分片由一个单进程 worker 消费——同 code 同类型天然串行（不会被两个进程
并发抓取），且复用该进程的 baostock 连接。

启动 worker（项目根目录，每个分片一个，shard 编号 0 ~ worker_shards-1）:
    uv run celery -A fetcher.app worker -Q shard0 -n shard0@%h -c 1 --loglevel=info
    uv run celery -A fetcher.app worker -Q shard1 -n shard1@%h -c 1 --loglevel=info
    uv run celery -A fetcher.app worker -Q shard2 -n shard2@%h -c 1 --loglevel=info
"""

import logging
import zlib

from celery import Celery
from celery.signals import worker_process_shutdown

from settings import settings

logger = logging.getLogger(__name__)


def _route_task(name, args, kwargs, options, task=None, **kw):
    """分片路由：亲和键取 code（或 index_code/kind），无 code 的任务按类型聚合。"""
    if not name.startswith("fetcher."):
        return None
    k = kwargs or {}
    affinity = k.get("code") or k.get("index_code") or k.get("kind") or ""
    shard = zlib.crc32(f"{name}:{affinity}".encode()) % settings.worker_shards
    return {"queue": f"shard{shard}"}

app = Celery(
    "stockdata",
    broker=settings.broker_url,
    backend=settings.result_backend,
    include=["fetcher.tasks", "fetcher.beat"],
)

app.conf.update(
    # 分片路由（亲和性，见模块 docstring）
    task_routes=(_route_task,),
    task_default_queue="shard0",
    # 子进程生命周期
    worker_concurrency=settings.worker_concurrency,
    worker_max_tasks_per_child=settings.worker_max_tasks_per_child,
    task_time_limit=settings.task_time_limit,
    task_soft_time_limit=settings.task_soft_time_limit,
    # 可靠性：子进程被 kill 后任务重回队列
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    # acks_late 的标准搭配：默认预取 4 时，连续慢任务会让被预取消息 unacked
    # 超过 visibility_timeout 而被重复投递执行
    worker_prefetch_multiplier=1,
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
    "sync-tracked-codes-after-close": {
        "task": "fetcher.beat.sync_tracked_codes",
        "schedule": crontab(hour=17, minute=10, day_of_week="1-5"),
    },
}


# 不在 worker_process_init 里登录 baostock：bs.login() 在慢网络下阻塞超过
# billiard 的 proc_alive_timeout（4s）时，父进程会判定子进程启动失败而杀掉重
# fork，陷入 fork→登录阻塞→被杀 的循环，整个分片瘫痪。登录惰性化到任务查询
# 路径上的 ensure_login()，失败走 Celery 的重试/失败路径；父进程仍不接触 baostock。


@worker_process_shutdown.connect
def _shutdown_child(**kwargs):
    from fetcher.providers import baostock as provider

    provider.logout()
