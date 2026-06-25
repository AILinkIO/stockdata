"""
内部抓取 worker：单线程串行消费 job，复用 providers.baostock（限流 + 长会话 + 登录重试）。

退避重试复刻旧 fetcher.tasks._run：DataSourceError 指数退避（base*2^n 封顶）最多
fetch_max_retries 次，耗尽才 failed；NoDataFoundError 是合法空结果 → done 空 payload。
单 worker 串行天然满足 baostock 单连接约束；进程长驻、不频繁重启（TASK §0 红线）。
"""

from __future__ import annotations

import logging
import threading
import time

import pandas as pd

from fetcher.providers import baostock as provider
from fetcher.providers.interface import DataSourceError, NoDataFoundError
from settings import settings

from .jobs import JobStore

logger = logging.getLogger(__name__)


def _backoff_seconds(attempt: int) -> int:
    wait = settings.fetch_retry_base_seconds * (2 ** attempt)
    return min(wait, settings.fetch_retry_max_backoff_seconds)


def _df_to_payload(df: pd.DataFrame | None) -> dict:
    """DataFrame → {fields, rows}（baostock 全字符串，原样透传，dotnet 侧解析）。"""
    if df is None or len(df) == 0:
        cols = list(df.columns) if df is not None else []
        return {"fields": cols, "rows": []}
    fields = list(df.columns)
    rows = [
        [None if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v) for v in rec]
        for rec in df.itertuples(index=False, name=None)
    ]
    return {"fields": fields, "rows": rows}


def _query(task_type: str, params: dict) -> pd.DataFrame:
    """按 type 分发到 provider 查询。当前 P4 只需 fetch_kline（日线/周/月线）。"""
    if task_type == "fetch_kline":
        return provider.query_k_data(
            params["code"], params["start_date"], params["end_date"],
            params.get("frequency", "d"),
        )
    raise ValueError(f"不支持的抓取类型: {task_type}")


def _run_job(store: JobStore, job_id: str) -> None:
    task = store.task_of(job_id)
    if task is None:
        return
    task_type, params = task

    store.mark_running(job_id)
    for attempt in range(settings.fetch_max_retries + 1):
        try:
            df = _query(task_type, params)
            store.mark_done(job_id, _df_to_payload(df))
            return
        except NoDataFoundError:
            # 合法空结果（停牌/未发布）：done 空 payload，dotnet 据 claimable_last 处理水位
            store.mark_done(job_id, _df_to_payload(None))
            return
        except DataSourceError as e:
            if attempt < settings.fetch_max_retries:
                wait = _backoff_seconds(attempt)
                logger.warning("抓取失败(第%d次)，%ds 后重试: %s", attempt + 1, wait, e)
                store.mark_running(job_id)  # 续期心跳
                time.sleep(wait)
                continue
            store.mark_failed(job_id, str(e))
            return
        except Exception as e:  # noqa: BLE001
            store.mark_failed(job_id, str(e))
            return


def _loop(store: JobStore, stop: threading.Event) -> None:
    logger.info("抓取 worker 已启动（串行消费 fetch:pending）")
    while not stop.is_set():
        try:
            job_id = store.next_pending(timeout=5)
            if job_id:
                _run_job(store, job_id)
        except Exception:  # noqa: BLE001
            logger.exception("worker 循环异常，继续")
            time.sleep(1)


def start_worker(store: JobStore) -> threading.Event:
    """启动单个 daemon worker 线程，返回 stop 事件。"""
    stop = threading.Event()
    threading.Thread(target=_loop, args=(store, stop), daemon=True, name="fetch-worker").start()
    return stop
