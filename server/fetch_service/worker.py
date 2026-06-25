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
from fetcher.providers.interface import BlacklistError, DataSourceError, NoDataFoundError
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


def _query(task_type: str, params: dict) -> dict:
    """按 type 分发到 provider 查询，返回 payload {fields, rows}。

    多数类型 provider 返回 DataFrame → _df_to_payload。财报季度类例外：query_fina_quarter
    返回 {report_type: record} dict（非表格），编码为 fields=[report_type, record]、每行
    [类型, json(记录)]，dotnet 侧拆解 record 提 stat_date/pub_date、其余进 metrics。
    """
    if task_type == "fetch_kline":
        return _df_to_payload(provider.query_k_data(
            params["code"], params["start_date"], params["end_date"], params.get("frequency", "d")))
    if task_type == "fetch_trade_calendar":
        return _df_to_payload(provider.query_trade_dates(params["start_date"], params["end_date"]))
    if task_type == "fetch_stock_basic":
        return _df_to_payload(provider.query_stock_basic(params["code"]))
    if task_type == "fetch_stock_list":
        return _df_to_payload(provider.query_all_stock(params["snap_date"]))
    if task_type == "fetch_industry":
        return _df_to_payload(provider.query_industry(params["snap_date"]))
    if task_type == "fetch_index_constituent":
        return _df_to_payload(provider.query_index_constituent(params["index_code"], params["snap_date"]))
    if task_type == "fetch_adjust_factor":
        # 恒全量：dotnet 侧已传 start=A股开市日，整段抓取（fore 随新除权全表重算）
        return _df_to_payload(provider.query_adjust_factor(params["code"], params["start_date"], params["end_date"]))
    if task_type == "fetch_dividend":
        return _df_to_payload(provider.query_dividend(params["code"], params["year"], params["year_type"]))
    if task_type == "fetch_macro":
        return _df_to_payload(provider.query_macro(params["kind"], params["start_date"], params["end_date"]))
    if task_type == "fetch_performance":
        # 业绩快报(express)/预告(forecast)：按 report_type 分发，表格
        q = provider.query_performance_express if params["report_type"] == "express" else provider.query_forecast
        return _df_to_payload(q(params["code"], params["start_date"], params["end_date"]))
    if task_type == "fetch_financial_report":
        import json
        cats = provider.query_fina_quarter(params["code"], params["year"], int(params["quarter"]))
        rows = [[rt, json.dumps(rec, default=str, ensure_ascii=False)] for rt, rec in cats.items()]
        return {"fields": ["report_type", "record"], "rows": rows}
    raise ValueError(f"不支持的抓取类型: {task_type}")


def _run_job(store: JobStore, job_id: str, stop: threading.Event) -> None:
    task = store.task_of(job_id)
    if task is None:
        return
    task_type, params = task

    store.mark_running(job_id)
    for attempt in range(settings.fetch_max_retries + 1):
        if stop.is_set():
            return  # 关停：放弃重试，不再触发登录（job 留 running，由 TTL/重投回收）
        try:
            store.mark_done(job_id, _query(task_type, params))
            return
        except NoDataFoundError:
            # 合法空结果（停牌/未发布）：done 空 payload，dotnet 据 claimable_last 处理水位
            store.mark_done(job_id, _df_to_payload(None))
            return
        except BlacklistError as e:
            # 出口 IP 被拉黑/持续接收错误：短期取不到数，重试只会延长封禁。
            # 当前 job 标记 failed + 写 halted 标志暂停抓取（不退进程，HTTP 保活），
            # _loop 据标志停止消费；待 MCP/前端调 POST /restart 清标志后恢复。
            logger.error("baostock 拉黑/接收错误，暂停抓取（待 /restart 恢复）: %s", e)
            store.mark_failed(job_id, str(e))
            store.set_halted(str(e))
            return
        except DataSourceError as e:
            if attempt < settings.fetch_max_retries:
                wait = _backoff_seconds(attempt)
                logger.warning("抓取失败(第%d次)，%ds 后重试: %s", attempt + 1, wait, e)
                store.mark_running(job_id)  # 续期心跳
                if stop.wait(wait):         # 可中断退避：关停立即返回，不再重连重试
                    return
                continue
            store.mark_failed(job_id, str(e))
            return
        except Exception as e:  # noqa: BLE001
            store.mark_failed(job_id, str(e))
            return


def _loop(store: JobStore, stop: threading.Event) -> None:
    logger.info("抓取 worker 已启动（串行消费 fetch:pending）")
    halted_logged = False
    while not stop.is_set():
        try:
            if store.halted_state():
                # 已暂停（拉黑）：不消费、不触登录，空转等 /restart 清标志（含容器重启后保留）
                if not halted_logged:
                    logger.warning("抓取处于暂停态（halted），等待 /restart 恢复")
                    halted_logged = True
                if stop.wait(2):
                    break
                continue
            halted_logged = False
            job_id = store.next_pending(timeout=5)
            if job_id:
                _run_job(store, job_id, stop)
        except Exception:  # noqa: BLE001
            logger.exception("worker 循环异常，继续")
            time.sleep(1)
    logger.info("抓取 worker 已停止")


def start_worker(store: JobStore) -> tuple[threading.Event, threading.Thread]:
    """启动单个 daemon worker 线程，返回 (stop 事件, 线程)。"""
    stop = threading.Event()
    thread = threading.Thread(target=_loop, args=(store, stop), daemon=True, name="fetch-worker")
    thread.start()
    return stop, thread
