import logging
import threading

from celery.apps.beat import Beat
from celery.apps.worker import Worker
from sqlalchemy import select
from sqlalchemy import update as sa_update

from db.models import FetchTask, TaskStatus
from db.session import SyncSession
from fetcher.app import app as celery_app

logger = logging.getLogger(__name__)


def _reconcile() -> None:
    """启动对账：补投上一个进程遗留的未完成任务。

    submit() 里『insert fetch_task 行』与『send_task 投递到 broker』是两步、不原子。
    若进程在两步之间崩溃（或 broker 丢消息），会留下『有行、无消息』的孤儿——
    pending 行无人执行、running 行半路夭折，且 pending 不触发僵尸清理，会永久占住
    params_hash 唯一索引、让同参数请求一直撞去重等到超时。

    单实例部署下，进程刚启动时不存在正在执行的任务：把所有未完成行重置为 pending 并
    重新投递一次，保证每条未完成任务都有 broker 消息。任务幂等（writer upsert + 水位
    单调），即使原消息仍在队列造成重复投递，也只是多抓一次，不会写坏数据。
    """
    with SyncSession.begin() as s:
        s.execute(
            sa_update(FetchTask)
            .where(FetchTask.status == TaskStatus.RUNNING)
            .values(status=TaskStatus.PENDING, started_at=None)
        )
        rows = s.execute(
            select(FetchTask.id, FetchTask.task_type, FetchTask.params)
            .where(FetchTask.status == TaskStatus.PENDING)
            .order_by(FetchTask.id)
        ).all()

    redispatched = 0
    for r in rows:
        try:
            celery_app.send_task(r.task_type, kwargs={**r.params, "fetch_task_id": r.id})
            redispatched += 1
        except Exception:
            logger.exception("启动对账重投失败 fetch_task #%s", r.id)
    if rows:
        logger.info("启动对账：重投 %d/%d 条未完成任务", redispatched, len(rows))


def start_embedded() -> None:
    def _run_worker():
        # 先对账补投遗留任务，再开始消费（send_task 只依赖 broker，不依赖 worker 自身）
        try:
            _reconcile()
        except Exception:
            logger.exception("启动对账失败，跳过（不影响 worker 正常消费新任务）")
        Worker(
            app=celery_app,
            pool="solo",
            concurrency=1,
            loglevel="info",
        ).start()

    def _run_beat():
        Beat(
            app=celery_app,
            schedule="/tmp/celerybeat-schedule",
            loglevel="info",
        ).start_scheduler()

    threading.Thread(target=_run_worker, daemon=True, name="celery-worker").start()
    threading.Thread(target=_run_beat, daemon=True, name="celery-beat").start()
    logger.info("嵌入式 Celery worker + beat 已启动（solo pool, daemon threads）")
