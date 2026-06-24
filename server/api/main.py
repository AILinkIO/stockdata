"""
A股数据 Web 服务入口。

启动方式（本工程目录 server/ 下）:
    uv run uvicorn api.main:app --host 0.0.0.0 --port 8080

接口文档: http://localhost:8080/docs

实现说明：路由为同步函数（FastAPI 自动调度到线程池），读穿透中等待
Celery 结果的阻塞不影响事件循环；数据访问使用同步 SQLAlchemy session。
Celery worker + beat 以 solo pool 运行在 API 进程的 daemon thread 中（嵌入式），
无需独立 worker / beat 进程。
"""

import logging
from contextlib import asynccontextmanager
from importlib.metadata import version as _pkg_version

from fastapi import FastAPI

from api.errors import register_exception_handlers
from api.routers import dates, financials, indices, macro, market, stocks, tasks, utils

_VERSION = _pkg_version("stockdata")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    from fetcher.worker import start_embedded
    start_embedded()
    yield


app = FastAPI(
    title="stockdata",
    description="中国 A 股市场数据服务（Baostock 数据源 + PostgreSQL 数据仓库）",
    version=_VERSION,
    lifespan=lifespan,
)

register_exception_handlers(app)

app.include_router(stocks.router)
app.include_router(financials.router)
app.include_router(indices.router)
app.include_router(market.router)
app.include_router(macro.router)
app.include_router(dates.router)
app.include_router(utils.router)
app.include_router(tasks.router)


@app.get("/healthz", tags=["meta"])
def healthz():
    return {"status": "ok", "name": "stockdata-server", "version": _VERSION}
