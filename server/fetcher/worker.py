import logging
import threading

from celery.apps.worker import Worker

from fetcher.app import app as celery_app

logger = logging.getLogger(__name__)


def start_embedded() -> None:
    def _run():
        Worker(
            app=celery_app,
            pool="solo",
            concurrency=1,
            loglevel="info",
        ).start()

    threading.Thread(target=_run, daemon=True, name="celery-worker").start()
    logger.info("嵌入式 Celery worker 已启动（solo pool, daemon thread）")
