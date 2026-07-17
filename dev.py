"""开发热重载入口：uv run python dev.py（生产部署仍是 docker + `stockdata serve`）。

改动 src/ 下文件后 uvicorn 自动重启进程、浏览器自动刷新。
对 baostock 是安全的：登录惰性（启动不触网，首个同步任务才 login），
且 ≥5min 登录间隔红线由 PG（baostock_session 表）持久化强制，跨进程
重启依然生效——热重载不会导致 baostock 反复重连。

注意：与 docker 容器同抢 :8050，开发前先 ./down.sh 停掉容器。
"""

from __future__ import annotations

import logging

from nicegui import app as fastapi_app
from nicegui import ui

from stockdata.config import settings
from stockdata.web.app import init_runner, shutdown_runner
from stockdata.web.pages import chart, home, sync  # noqa: F401  # import 即注册页面

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

fastapi_app.on_startup(init_runner)
fastapi_app.on_shutdown(shutdown_runner)

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        host=settings.web_host,
        port=settings.web_port,
        title="stockdata (dev)",
        reload=True,
        uvicorn_reload_dirs="src",  # 只盯 src/，避免根目录杂文件触发无谓重启
        show=False,
        favicon="🛠️",
        fastapi_docs=True,
    )
