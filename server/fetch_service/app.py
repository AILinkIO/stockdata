"""
抓取微服务 HTTP 入口（异步 submit + poll，TASK D-A）。

启动（server/ 目录下）:
    uv run uvicorn fetch_service.app:app --host 0.0.0.0 --port 8090

  POST /fetch          {type, params}        → 202 {job_id, status, dedup}
  GET  /fetch/{job_id}                        → {job_id, status, payload?, error?}
  GET  /status                                → {worker: running|halted, halted?}
  POST /restart                               → 清暂停标志、恢复抓取
  GET  /healthz

抓取暂停（halted）：baostock 拉黑/接收错误（10001011/10002007）时 worker 写持久标志、
停止消费（进程不退、HTTP 保活）。MCP/前端经 GET /status 感知，经 POST /restart 恢复。

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
    priority: str = "low"   # high（MCP 交互读，插队）/ low（后台全量同步）


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
    job_id, status, dedup = _store.submit(req.type, req.params, req.priority)
    return {"job_id": job_id, "status": status, "dedup": dedup}


@app.get("/fetch/{job_id}")
def get_job(job_id: str, response: Response):
    job = _store.get(job_id)
    if job is None:
        response.status_code = 404
        return {"job_id": job_id, "status": "failed", "payload": None, "error": "job 不存在或已过期"}
    return job


@app.get("/status")
def status():
    """抓取状态：worker 是否因拉黑暂停。供 MCP/前端感知后台是否停摆。"""
    halted = _store.halted_state()
    return {
        "name": "stockdata-fetch",
        "worker": "halted" if halted else "running",
        "halted": halted,  # {reason, since} 或 null
    }


@app.post("/restart")
def restart():
    """清除暂停标志、恢复抓取（拉黑解除/换出口 IP 后由 MCP 或前端调用）。

    清标志 → 丢弃可能僵死的 baostock 会话（下个 job 重新登录）。worker 线程一直在
    空转，标志一清即恢复消费；无需重启进程/容器。
    """
    was = _store.halted_state()
    _store.clear_halted()
    provider.reset_login_state()
    logger.info("收到 /restart：清除暂停标志，恢复抓取（原因曾为: %s）", was.get("reason") if was else None)
    return {"status": "ok", "worker": "running", "was_halted": was is not None, "previous": was}


@app.get("/healthz")
def healthz():
    return {"status": "ok", "name": "stockdata-fetch"}
