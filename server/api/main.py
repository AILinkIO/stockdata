"""
A股数据 Web 服务入口。

启动方式（本工程目录 server/ 下）:
    uv run uvicorn api.main:app --host 0.0.0.0 --port 8000

接口文档: http://localhost:8000/docs

实现说明：路由为同步函数（FastAPI 自动调度到线程池），读穿透中等待
Celery 结果的阻塞不影响事件循环；数据访问使用同步 SQLAlchemy session。
"""

import logging

from fastapi import FastAPI

from api.errors import register_exception_handlers
from api.routers import dates, financials, indices, macro, market, stocks, tasks, utils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

app = FastAPI(
    title="stockdata",
    description="中国 A 股市场数据服务（Baostock 数据源 + PostgreSQL 数据仓库）",
    version="1.0.0",
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
    return {"status": "ok"}
