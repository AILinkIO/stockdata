"""NiceGUI 单服务入口：Web 页面 + /api/sync/* REST + 唯一 baostock 同步 worker。"""

from __future__ import annotations

import logging

from fastapi import HTTPException
from nicegui import app as fastapi_app
from nicegui import ui
from pydantic import BaseModel

from stockdata.config import settings
from stockdata.db import queries
from stockdata.sync.engine import (
    RunParams, clear_halt, read_halt, recover_interrupted_run,
)

from . import state
from .api_v1 import router as api_v1_router

fastapi_app.include_router(api_v1_router)

logger = logging.getLogger(__name__)


class RunRequest(BaseModel):
    codes: list[str] = []
    datasets: list[str] = []
    watchlist_only: bool = False


# ── REST API（CLI 客户端与页面共用）──


@fastapi_app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "name": "stockdata"}


@fastapi_app.get("/metrics")
def metrics() -> "PlainTextResponse":
    """Prometheus 抓取端点：同步状态、调用速率、熔断、各数据集水位年龄。"""
    from datetime import date

    from fastapi.responses import PlainTextResponse

    lines: list[str] = []

    def m(name: str, value, labels: str = "", mtype: str = "gauge") -> None:
        if not any(line.startswith(f"# TYPE {name} ") for line in lines):
            lines.append(f"# TYPE {name} {mtype}")
        lines.append(f"{name}{labels} {value}")

    st = state.runner.state() if state.runner is not None else None
    m("stockdata_sync_running", int(st["running"]) if st else 0)
    if st:
        m("stockdata_sync_calls_per_minute", st["calls_per_minute"])
        m("stockdata_sync_calls_total", st["calls_total"], mtype="counter")
        m("stockdata_sync_run_errors", len(st["errors"]))
    halt = _read_halt()
    kind = (halt or {}).get("kind", "")
    m("stockdata_halt", int(halt is not None), f'{{kind="{kind}"}}')
    try:
        summary = queries.watermark_summary()
        for d in summary["datasets"]:
            label = f'{{dataset="{d["dataset"]}"}}'
            m("stockdata_watermark_codes", d["codes"], label)
            if d["max_last"]:
                age = (date.today() - date.fromisoformat(d["max_last"])).days
                m("stockdata_watermark_age_days", age, label)
    except Exception:
        logger.exception("metrics 水位查询失败")
    return PlainTextResponse("\n".join(lines) + "\n")


@fastapi_app.get("/api/sync/status")
def sync_status() -> dict:
    return {
        "state": state.get_runner().state(),
        "halt": _read_halt(),
    }


@fastapi_app.get("/api/sync/overview")
def sync_overview() -> dict:
    return {
        "watermarks": queries.watermark_summary(),
        "runs": queries.recent_runs(10),
    }


@fastapi_app.post("/api/sync/run", status_code=202)
def sync_run(req: RunRequest) -> dict:
    halt = _read_halt()
    if halt:
        raise HTTPException(409, f"处于熔断状态：{halt.get('reason', '?')}（先 clear-halt）")
    ok, msg = state.get_runner().start(RunParams(
        codes=req.codes, datasets=req.datasets, watchlist_only=req.watchlist_only,
    ))
    if not ok:
        raise HTTPException(409, msg)
    return {"message": msg}


@fastapi_app.post("/api/sync/stop")
def sync_stop() -> dict:
    stopped = state.get_runner().stop()
    return {"stopping": stopped}


@fastapi_app.post("/api/sync/clear-halt")
def sync_clear_halt() -> dict:
    cleared = clear_halt(settings.pg_conninfo)
    return {"cleared": cleared}


def _read_halt() -> dict | None:
    import psycopg

    with psycopg.connect(settings.pg_conninfo) as conn:
        return read_halt(conn)


# ── 生命周期 ──


def init_runner(provider=None) -> None:
    """构造唯一 Provider + SyncRunner。provider 可注入（测试用 FakeProvider）。"""
    if state.runner is not None:
        return
    if provider is None:
        from stockdata.core.ratelimit import MemoryRateLimiter
        from stockdata.provider.baostock import BaostockProvider
        from stockdata.provider.session_guard import PgSessionStore, SessionGuard

        guard = SessionGuard(
            PgSessionStore(settings.pg_conninfo), settings.min_login_interval_seconds
        )
        provider = BaostockProvider(
            settings, guard, MemoryRateLimiter(settings.rate_limit_per_minute)
        )
    from stockdata.sync.runner import SyncRunner

    state.runner = SyncRunner(settings.pg_conninfo, provider, settings)
    logger.info("SyncRunner 已启动（唯一 baostock worker 线程）")
    _resume_interrupted()


def _resume_interrupted() -> None:
    """崩溃恢复：孤儿 running 收尾；最新一条 interrupted 自动续跑（水位断点续传）。"""
    try:
        params = recover_interrupted_run(settings.pg_conninfo)
    except Exception:
        logger.exception("启动收尾失败")
        return
    if params is None:
        return
    if not settings.resume_interrupted_on_start:
        logger.info("发现被中断的同步任务，但自动续跑已关闭（resume_interrupted_on_start）")
        return
    if _read_halt():
        logger.warning("发现被中断的同步任务，但处于熔断状态，不自动续跑")
        return
    ok, msg = state.runner.start(RunParams(
        codes=params.get("codes", []),
        datasets=params.get("datasets", []),
        watchlist_only=params.get("watchlist_only", False),
    ))
    logger.info("自动续跑被中断的同步任务 %s：%s", params, msg)


def shutdown_runner() -> None:
    if state.runner is not None:
        state.runner.shutdown()
        state.runner = None


def run_app() -> None:
    """`stockdata serve` 入口。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # 页面注册（import 即注册 @ui.page）
    from .pages import chart, home, sync  # noqa: F401

    fastapi_app.on_startup(init_runner)
    fastapi_app.on_shutdown(shutdown_runner)
    ui.run(
        host=settings.web_host,
        port=settings.web_port,
        title="stockdata",
        reload=False,
        show=False,
        favicon="📈",
        fastapi_docs=True,  # /docs、/openapi.json（数据面 API 文档）
    )
