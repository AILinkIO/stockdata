"""异步批量回填接口（202 + 轮询）。"""

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.services.dispatch import submit
from db.models import FetchTask
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


@router.get("/{task_id}")
def get_task(task_id: int):
    with SyncSession() as s:
        row = s.get(FetchTask, task_id)
    if row is None:
        raise HTTPException(404, f"任务 {task_id} 不存在")
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
