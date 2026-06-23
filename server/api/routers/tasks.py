"""异步批量回填接口（202 + 轮询）。"""

from datetime import datetime, time as dtime
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, select

from api.services.dispatch import submit
from core.timeutil import CST, today_cst
from db.models import FetchTask, TaskStatus
from db.session import SyncSession

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])

_ALLOWED = {
    "fetcher.fetch_kline",
    "fetcher.fetch_kline_minute",
    "fetcher.fetch_adjust_factor",
    "fetcher.fetch_stock_basic",
    "fetcher.fetch_dividend",
    "fetcher.fetch_financial_report",
    "fetcher.fetch_performance_report",
    "fetcher.fetch_trade_calendar",
    "fetcher.fetch_stock_list",
    "fetcher.fetch_index_constituent",
    "fetcher.fetch_industry",
    "fetcher.fetch_macro",
}


class BackfillRequest(BaseModel):
    task: str
    params: dict[str, Any]


@router.post("/backfill", status_code=202)
def backfill(req: BackfillRequest):
    if req.task not in _ALLOWED:
        raise HTTPException(422, f"未知任务类型: {req.task}，可选: {sorted(_ALLOWED)}")
    row_id, celery_id = submit(req.task, req.params)
    if row_id is None:
        return {"detail": "同参数任务已在队列中", "task_id": None}
    return {"task_id": row_id, "celery_task_id": celery_id}


def _task_view(row: FetchTask) -> dict[str, Any]:
    return {
        "id": row.id,
        "task_type": row.task_type,
        "params": row.params,
        "status": row.status,
        "error": row.error,
        "created_at": row.created_at,
        "started_at": row.started_at,
        "finished_at": row.finished_at,
    }


@router.get("")
def list_tasks(
    status: TaskStatus | None = Query(None, description="按状态过滤：pending/running/succeeded/failed"),
    task_type: str | None = Query(None, description="按任务类型过滤，如 fetcher.fetch_kline"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """查询任务状态列表（按创建时间倒序，支持状态/类型过滤与分页）。"""
    stmt = select(FetchTask).order_by(FetchTask.id.desc())
    if status is not None:
        stmt = stmt.where(FetchTask.status == status)
    if task_type is not None:
        stmt = stmt.where(FetchTask.task_type == task_type)
    stmt = stmt.limit(limit).offset(offset)
    with SyncSession() as s:
        rows = s.execute(stmt).scalars().all()
    return {"items": [_task_view(r) for r in rows], "limit": limit, "offset": offset}


@router.get("/stats")
def task_stats():
    """任务统计：各状态总数、按任务类型分组、今日（CST）成功/失败数。"""
    day_start = datetime.combine(today_cst(), dtime.min, tzinfo=CST)
    with SyncSession() as s:
        by_status = dict(
            s.execute(
                select(FetchTask.status, func.count()).group_by(FetchTask.status)
            ).all()
        )
        by_task_type = dict(
            s.execute(
                select(FetchTask.task_type, func.count()).group_by(FetchTask.task_type)
            ).all()
        )
        today_rows = dict(
            s.execute(
                select(FetchTask.status, func.count())
                .where(
                    FetchTask.finished_at >= day_start,
                    FetchTask.status.in_([TaskStatus.SUCCEEDED, TaskStatus.FAILED]),
                )
                .group_by(FetchTask.status)
            ).all()
        )
    return {
        "total": sum(by_status.values()),
        "by_status": {st.value: by_status.get(st.value, 0) for st in TaskStatus},
        "by_task_type": by_task_type,
        "today": {
            "succeeded": today_rows.get(TaskStatus.SUCCEEDED.value, 0),
            "failed": today_rows.get(TaskStatus.FAILED.value, 0),
        },
    }


@router.get("/running")
def running_tasks(limit: int = Query(200, ge=1, le=1000)):
    """当前在途任务（pending/running），按 id 升序（最早入队在前，便于发现卡住的任务）。"""
    stmt = (
        select(FetchTask)
        .where(FetchTask.status.in_([TaskStatus.PENDING, TaskStatus.RUNNING]))
        .order_by(FetchTask.id.asc())
        .limit(limit)
    )
    with SyncSession() as s:
        rows = s.execute(stmt).scalars().all()
    return {"items": [_task_view(r) for r in rows], "count": len(rows)}


@router.get("/{task_id}")
def get_task(task_id: int):
    with SyncSession() as s:
        row = s.get(FetchTask, task_id)
    if row is None:
        raise HTTPException(404, f"任务 {task_id} 不存在")
    return _task_view(row)
