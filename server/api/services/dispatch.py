"""
任务投递与等待：fetch_task 记录（去重）→ Celery 投递 → 阻塞等待结果。

本模块在 FastAPI 同步路由（线程池）中调用，result.get() 不会阻塞事件循环。
去重依赖 fetch_task 的部分唯一索引：同参数的 pending/running 任务只允许一个，
撞到重复时轮询等待已有任务完成。
"""

import hashlib
import json
import logging
import time

from celery.exceptions import TimeoutError as CeleryTimeoutError
from sqlalchemy import select, text
from sqlalchemy import update as sa_update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from api.errors import FetchFailedError, FetchTimeoutError
from db.models import FetchTask, TaskStatus
from db.session import SyncSession
from fetcher.app import app as celery_app
from settings import settings

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 0.5


def _params_hash(task_name: str, params: dict) -> str:
    payload = json.dumps({"task": task_name, **params}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def submit(task_name: str, params: dict) -> tuple[int | None, str | None]:
    """记录并投递任务。返回 (fetch_task_id, celery_task_id)；重复任务返回 (None, None)。"""
    h = _params_hash(task_name, params)
    with SyncSession.begin() as s:
        row_id = s.execute(
            pg_insert(FetchTask.__table__)
            .values(task_type=task_name, params=params, params_hash=h)
            .on_conflict_do_nothing(
                index_elements=["params_hash"],
                index_where=text("status IN ('pending', 'running')"),
            )
            .returning(FetchTask.__table__.c.id)
        ).scalar()
    if row_id is None:
        return None, None  # 同参数任务已在队列中
    result = celery_app.send_task(task_name, kwargs={**params, "fetch_task_id": row_id})
    # 投递后立即回写 celery_task_id（_run 在任务开始执行时才写），
    # 让 pending 行也能关联到 broker 消息，便于观测与僵尸排查
    with SyncSession.begin() as s:
        s.execute(
            sa_update(FetchTask)
            .where(FetchTask.id == row_id)
            .values(celery_task_id=result.id)
        )
    return row_id, result.id


def dispatch_and_wait(task_name: str, params: dict, timeout: float | None = None) -> None:
    """投递并等待完成；重复任务则等待已有任务。超时抛 FetchTimeoutError。"""
    timeout = timeout if timeout is not None else settings.fetch_wait_timeout
    row_id, celery_id = submit(task_name, params)

    if row_id is None:
        _wait_existing(_params_hash(task_name, params), timeout, task_name)
        return

    try:
        celery_app.AsyncResult(celery_id).get(timeout=timeout, propagate=True)
    except CeleryTimeoutError as e:
        raise FetchTimeoutError(f"{task_name} {params}") from e
    except Exception as e:
        # 子进程被 SIGKILL 时任务内的状态标记没有机会执行，在此兜底标记 failed，
        # 否则部分唯一索引会永久挡住同参数任务的重新投递
        _mark_failed(row_id, str(e))
        raise FetchFailedError(f"{task_name} {params}: {e}") from e


def _mark_failed(row_id: int, error: str) -> None:
    with SyncSession.begin() as s:
        s.execute(
            sa_update(FetchTask)
            .where(FetchTask.id == row_id,
                   FetchTask.status.in_([TaskStatus.PENDING, TaskStatus.RUNNING]))
            .values(status=TaskStatus.FAILED, error=error)
        )


def _wait_existing(params_hash: str, timeout: float, task_name: str) -> None:
    """轮询等待同参数的已有任务离开 pending/running 状态。

    僵尸防护：
    - running 行的 started_at 超过 task_time_limit×2 视为其子进程已被
      SIGKILL 且无人收尸，标记 failed 释放唯一索引，让客户端重试时能重新投递。
    - pending 行的 created_at 超过 visibility_timeout + task_time_limit 视为
      broker 消息已丢失（消息若还在，重投递窗口内必然已被执行或转入 running），
      同样标记 failed。误杀积压任务的代价可接受：写入幂等，重试会重新投递。
    """
    from datetime import datetime, timedelta, timezone

    deadline = time.monotonic() + timeout
    running_stuck = timedelta(seconds=settings.task_time_limit * 2)
    pending_stuck = timedelta(seconds=settings.visibility_timeout + settings.task_time_limit)
    while time.monotonic() < deadline:
        with SyncSession() as s:
            row = s.execute(
                select(FetchTask.id, FetchTask.status,
                       FetchTask.started_at, FetchTask.created_at)
                .where(FetchTask.params_hash == params_hash)
                .order_by(FetchTask.id.desc())
                .limit(1)
            ).first()
        if row is None or row.status not in (TaskStatus.PENDING, TaskStatus.RUNNING):
            if row is not None and row.status == TaskStatus.FAILED:
                raise FetchFailedError(f"{task_name}: 已有同参数任务执行失败")
            return
        now = datetime.now(timezone.utc)
        if (
            row.status == TaskStatus.RUNNING
            and row.started_at is not None
            and now - row.started_at > running_stuck
        ):
            _mark_failed(row.id, "僵尸任务：子进程疑似被 SIGKILL，由等待方标记失败")
            raise FetchFailedError(f"{task_name}: 已有同参数任务僵死，已清理，请重试")
        if row.status == TaskStatus.PENDING and now - row.created_at > pending_stuck:
            _mark_failed(row.id, "僵尸任务：broker 消息疑似丢失，由等待方标记失败")
            raise FetchFailedError(f"{task_name}: 已有同参数任务消息丢失，已清理，请重试")
        time.sleep(_POLL_INTERVAL)
    raise FetchTimeoutError(f"{task_name}: 等待已有任务超时")
