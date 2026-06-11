"""领域异常 → HTTP 状态码映射（设计文档 6.3 节，取代旧 tool_runner.py）。"""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from fetcher.providers.interface import DataSourceError, LoginError, NoDataFoundError

logger = logging.getLogger(__name__)


class FetchTimeoutError(Exception):
    """等待抓取任务完成超时。任务仍在后台执行，客户端稍后重试即可命中。"""


class FetchFailedError(Exception):
    """抓取任务执行失败（重试耗尽）。"""


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(NoDataFoundError)
    def _no_data(request: Request, exc: NoDataFoundError):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(FetchTimeoutError)
    def _timeout(request: Request, exc: FetchTimeoutError):
        return JSONResponse(
            status_code=504,
            content={"detail": f"数据抓取超时，后台仍在处理，请稍后重试。{exc}"},
        )

    @app.exception_handler(FetchFailedError)
    def _failed(request: Request, exc: FetchFailedError):
        return JSONResponse(status_code=502, content={"detail": f"数据源抓取失败: {exc}"})

    @app.exception_handler(LoginError)
    def _login(request: Request, exc: LoginError):
        return JSONResponse(status_code=502, content={"detail": f"数据源连接失败: {exc}"})

    @app.exception_handler(DataSourceError)
    def _ds(request: Request, exc: DataSourceError):
        return JSONResponse(status_code=502, content={"detail": f"数据源错误: {exc}"})

    @app.exception_handler(ValueError)
    def _value(request: Request, exc: ValueError):
        return JSONResponse(status_code=422, content={"detail": str(exc)})
