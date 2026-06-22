"""
任务投递与等待。

submit() 经 Celery send_task() 异步投递到 Redis 队列；dispatch_and_wait() 通过轮询
fetch_task 状态行等待完成（不依赖 Celery result backend）。去重依赖 fetch_task 的部分
唯一索引：同参数的 pending/running 任务只允许一个，撞到重复时等待已有任务完成。

为何不用 AsyncResult.get()：嵌入式 worker 与 API 同进程，worker 启动会把 Celery 的
进程级全局 _task_join_will_block 置 True，令请求线程的 .get() 误判为"任务内调用"而
抛错；且 fetch_task 状态表已是权威的完成/失败信号，result backend 对等待属冗余。
"""

import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone

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
# 僵尸判定阈值：必须 > broker 重投窗口(visibility_timeout=600s) > 最坏单任务时长
# （单次 baostock 调用 + 最多 2 次退避重试，约 100~120s）。否则会误杀仍在健康执行的
# 长任务——把 running 行翻成 failed、释放 params_hash 唯一索引，引发重复抓取与假 502。
# 留足余量后，等待方只在「撞去重、加入一个早已运行很久的任务」时才可能触发它
# （owner 的 started_at≈now，在 fetch_wait_timeout 窗口内根本够不到）。
_STALE_RUNNING = timedelta(seconds=1200)


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
    """投递并等待完成；重复任务则等待已有任务。超时抛 FetchTimeoutError，
    任务失败抛 FetchFailedError。"""
    timeout = timeout if timeout is not None else settings.fetch_wait_timeout
    row_id, _celery_id = submit(task_name, params)

    if row_id is None:
        # 撞到去重：等待已有同参数任务（取该 hash 最新一行）
        row_id = _existing_row_id(_params_hash(task_name, params))
        if row_id is None:
            return  # 已有任务在投递与查询之间结束，视为完成
    _wait_done(row_id, timeout, task_name)


def _mark_failed(row_id: int, error: str) -> None:
    with SyncSession.begin() as s:
        s.execute(
            sa_update(FetchTask)
            .where(FetchTask.id == row_id,
                   FetchTask.status.in_([TaskStatus.PENDING, TaskStatus.RUNNING]))
            .values(status=TaskStatus.FAILED, error=error)
        )


def _existing_row_id(params_hash: str) -> int | None:
    """取同参数 hash 的最新 fetch_task 行 id（撞去重时已有在途任务即此行）。"""
    with SyncSession() as s:
        return s.execute(
            select(FetchTask.id)
            .where(FetchTask.params_hash == params_hash)
            .order_by(FetchTask.id.desc())
            .limit(1)
        ).scalar()


def _wait_done(row_id: int, timeout: float, task_name: str) -> None:
    """轮询 fetch_task 状态直至离开 pending/running。

    succeeded → 返回；failed → 抛 FetchFailedError（带任务写入的 error）。
    僵尸防护：running 行超过 _STALE_RUNNING 视为 worker 异常（崩溃/卡死），
    标记 failed 释放唯一索引，让客户端重试。
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with SyncSession() as s:
            row = s.execute(
                select(FetchTask.status, FetchTask.started_at, FetchTask.error)
                .where(FetchTask.id == row_id)
            ).first()
        if row is None or row.status == TaskStatus.SUCCEEDED:
            return
        if row.status == TaskStatus.FAILED:
            raise FetchFailedError(f"{task_name}: {row.error or '任务执行失败'}")
        if (
            row.status == TaskStatus.RUNNING
            and row.started_at is not None
            and datetime.now(timezone.utc) - row.started_at > _STALE_RUNNING
        ):
            _mark_failed(row_id, "僵尸任务：执行超时未完成，由等待方标记失败")
            raise FetchFailedError(f"{task_name}: 任务僵死，已清理，请重试")
        time.sleep(_POLL_INTERVAL)
    raise FetchTimeoutError(f"{task_name}: 等待任务超时")
