"""
Baostock 会话管理模块。

使用请求队列 + 后台 worker 线程串行化所有 baostock 操作。

Baostock 内部维护全局 TCP 连接（module-level 单例），不支持并发访问。
"""

import os
import sys
import atexit
import logging
import threading
import queue
from contextlib import contextmanager
from typing import Any, Callable, TypeVar

import baostock as bs

from .interface import DataSourceError

logger = logging.getLogger(__name__)

T = TypeVar("T")

_SESSION_EXPIRED_CODE = "10001001"


def _is_session_expired(exc: Exception) -> bool:
    return (
        _SESSION_EXPIRED_CODE in str(exc)
        or "login" in str(exc).lower()
        or "未登录" in str(exc)
    )


@contextmanager
def _suppress_stdout():
    original_fd = sys.stdout.fileno()
    saved_fd = os.dup(original_fd)
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull_fd, original_fd)
    os.close(devnull_fd)
    try:
        yield
    finally:
        os.dup2(saved_fd, original_fd)
        os.close(saved_fd)


def _do_login() -> bool:
    with _suppress_stdout():
        lg = bs.login()
    if lg.error_code != "0":
        logger.error(f"Baostock 登录失败: {lg.error_msg}")
        return False
    logger.debug("Baostock 登录成功")
    return True


def _do_logout():
    with _suppress_stdout():
        bs.logout()
    logger.debug("Baostock 登出成功")


_request_queue: queue.Queue = queue.Queue()
_worker_thread: threading.Thread | None = None
_shutdown_event = threading.Event()


def _worker_loop():
    logger.info("Baostock worker 线程启动")

    if not _do_login():
        logger.error("Baostock worker 初始登录失败，worker 退出")
        return

    logger.info("Baostock worker 就绪，开始处理请求队列")

    while not _shutdown_event.is_set():
        try:
            work, result_queue = _request_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        for attempt in range(2):
            try:
                result = work()
                result_queue.put(("ok", result))
                break
            except Exception as e:
                if attempt == 0 and _is_session_expired(e):
                    logger.warning("Baostock 会话失效，正在重新登录")
                    _do_logout()
                    if _do_login():
                        continue
                result_queue.put(("err", e))
                break

        _request_queue.task_done()


def _start_worker():
    global _worker_thread
    if _worker_thread is not None and _worker_thread.is_alive():
        return
    _shutdown_event.clear()
    _worker_thread = threading.Thread(
        target=_worker_loop, daemon=True, name="baostock-worker"
    )
    _worker_thread.start()


def execute(work: Callable[[], T]) -> T:
    """提交 callable 到请求队列，阻塞等待结果。

    所有 baostock 操作必须通过此函数提交。
    """
    _start_worker()

    result_queue: queue.Queue = queue.Queue()
    _request_queue.put((work, result_queue))
    status, value = result_queue.get()

    if status == "err":
        raise value
    return value


@contextmanager
def baostock_session():
    yield


def relogin():
    pass


def _shutdown():
    _shutdown_event.set()
    if _worker_thread is not None and _worker_thread.is_alive():
        _worker_thread.join(timeout=5)
    try:
        _do_logout()
    except Exception:
        pass


atexit.register(_shutdown)
