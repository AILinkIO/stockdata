"""
任务投递与等待。

submit() 经 Celery send_task() 异步投递到 Redis 队列，dispatch_and_wait() 用
AsyncResult.get() 阻塞等待结果。去重依赖 fetch_task 的部分唯一索引：同参数的
pending/running 任务只允许一个，撞到重复时轮询等待已有任务完成。
"""

import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone

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
_STALE_RUNNING = timedelta(seconds=600)


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
        return None, None
    try:
        result = celery_app.send_task(task_name, kwargs={**params, "fetch_task_id": row_id})
    except Exception as e:
        _mark_failed(row_id, f"broker 投递失败: {e}")
        raise FetchFailedError(f"{task_name} {params}: broker 投递失败: {e}") from e
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

    僵尸防护：running 行超过 _STALE_RUNNING 视为 worker 异常（崩溃/卡死），
    标记 failed 释放唯一索引，让客户端重试。
    """
    deadline = time.monotonic() + timeout
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
            and now - row.started_at > _STALE_RUNNING
        ):
            _mark_failed(row.id, "僵尸任务：执行超时未完成，由等待方标记失败")
            raise FetchFailedError(f"{task_name}: 已有同参数任务僵死，已清理，请重试")
        time.sleep(_POLL_INTERVAL)
    raise FetchTimeoutError(f"{task_name}: 等待已有任务超时")
