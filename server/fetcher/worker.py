import logging
import threading

from celery.apps.beat import Beat
from celery.apps.worker import Worker

from fetcher.app import app as celery_app

logger = logging.getLogger(__name__)


def start_embedded() -> None:
    def _run_worker():
        Worker(
            app=celery_app,
            pool="solo",
            concurrency=1,
            loglevel="info",
        ).start()

    def _run_beat():
        Beat(
            app=celery_app,
            schedule="/tmp/celerybeat-schedule",
            loglevel="info",
        ).start_scheduler()

    threading.Thread(target=_run_worker, daemon=True, name="celery-worker").start()
    threading.Thread(target=_run_beat, daemon=True, name="celery-beat").start()
    logger.info("嵌入式 Celery worker + beat 已启动（solo pool, daemon threads）")
