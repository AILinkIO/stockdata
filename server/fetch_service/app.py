"""
抓取微服务 HTTP 入口（异步 submit + poll，TASK D-A）。

启动（server/ 目录下）:
    uv run uvicorn fetch_service.app:app --host 0.0.0.0 --port 8090

  POST /fetch          {type, params}        → 202 {job_id, status, dedup}
  GET  /fetch/{job_id}                        → {job_id, status, payload?, error?}
  GET  /healthz

进程长驻、单 worker 串行、单例 baostock 会话；**重启间隔 > 5 分钟**（TASK §0 红线）。
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from pydantic import BaseModel

from fetcher.providers import baostock as provider

from .jobs import JobStore
from .worker import start_worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

_store = JobStore()


class FetchRequest(BaseModel):
    type: str
    params: dict


@asynccontextmanager
async def lifespan(_app: FastAPI):
    stop, thread = start_worker(_store)
    try:
        yield
    finally:
        # 关停顺序：先停 worker（中断退避、不再重试登录）→ 等其退出释放 _BS_LOCK
        # → 干净登出 baostock（让服务端及时回收会话，下次登录不易被判异常重复会话）
        stop.set()
        thread.join(timeout=10)
        provider.logout()
        logger.info("已登出 baostock，进程退出")


app = FastAPI(title="stockdata-fetch", lifespan=lifespan)


@app.post("/fetch", status_code=202)
def submit(req: FetchRequest, response: Response):
    job_id, status, dedup = _store.submit(req.type, req.params)
    return {"job_id": job_id, "status": status, "dedup": dedup}


@app.get("/fetch/{job_id}")
def get_job(job_id: str, response: Response):
    job = _store.get(job_id)
    if job is None:
        response.status_code = 404
        return {"job_id": job_id, "status": "failed", "payload": None, "error": "job 不存在或已过期"}
    return job


@app.get("/healthz")
def healthz():
    return {"status": "ok", "name": "stockdata-fetch"}
