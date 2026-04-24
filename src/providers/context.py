"""
Baostock 会话管理模块。

使用请求队列 + 后台 worker 线程串行化所有 baostock 操作。

Baostock 内部维护全局 TCP 连接（module-level 单例），不支持并发访问。
"""

import os
import sys
import time
import atexit
import logging
import threading
import queue
from contextlib import contextmanager
from typing import Callable, TypeVar

import baostock as bs

from .interface import DataSourceError

logger = logging.getLogger(__name__)

T = TypeVar("T")

_REQUEST_TIMEOUT = 60  # execute() 等待 worker 响应的超时秒数
_WORKER_SHUTDOWN_JOIN = 5  # 关闭时等待 worker 退出的秒数
_INITIAL_LOGIN_RETRIES = 3  # worker 启动时初始登录的重试次数
_INITIAL_LOGIN_BACKOFF = 3  # 初始登录重试的退避秒数

# 需要通过重连重试才能恢复的错误码
_RETRYABLE_CODES = frozenset(
    {
        "10001001",  # 用户未登录
        "10002001",  # 网络错误
        "10002002",  # 网络连接失败
        "10002004",  # 连接断开
        "10002007",  # 网络接收错误
    }
)


def _is_retryable_error(exc: Exception) -> bool:
    msg = str(exc)
    return (
        any(code in msg for code in _RETRYABLE_CODES)
        or "login" in msg.lower()
        or "未登录" in msg
        or "Broken pipe" in msg
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
        logger.error("Baostock 登录失败: %s", lg.error_msg)
        return False
    logger.debug("Baostock 登录成功")
    return True


def _do_logout():
    with _suppress_stdout():
        bs.logout()
    logger.debug("Baostock 登出成功")


_request_queue: queue.Queue = queue.Queue()
_worker_thread: threading.Thread | None = None
_worker_ready = threading.Event()
_shutdown_event = threading.Event()


def _worker_loop():
    logger.info("Baostock worker 线程启动")

    for attempt in range(1, _INITIAL_LOGIN_RETRIES + 1):
        if _shutdown_event.is_set():
            logger.info("Baostock worker 在初始登录前收到关闭信号，退出")
            return
        if _do_login():
            break
        if attempt < _INITIAL_LOGIN_RETRIES:
            logger.warning(
                "Baostock 初始登录失败，%ss 后重试（%d/%d）",
                _INITIAL_LOGIN_BACKOFF,
                attempt,
                _INITIAL_LOGIN_RETRIES,
            )
            if _shutdown_event.wait(_INITIAL_LOGIN_BACKOFF):
                logger.info("Baostock worker 在重试等待期间收到关闭信号，退出")
                return
    else:
        logger.error(
            "Baostock worker 初始登录失败（已重试 %d 次），worker 退出",
            _INITIAL_LOGIN_RETRIES,
        )
        return

    _worker_ready.set()
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
                if attempt == 0 and _is_retryable_error(e):
                    logger.warning("Baostock 可重试错误，正在重新登录: %s", e)
                    time.sleep(1)
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
    _worker_ready.clear()
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

    try:
        status, value = result_queue.get(timeout=_REQUEST_TIMEOUT)
    except queue.Empty:
        raise DataSourceError(
            f"Baostock 请求超时（{_REQUEST_TIMEOUT}s），worker 线程可能已异常退出"
        )

    if status == "err":
        raise value
    return value


def _shutdown():
    _shutdown_event.set()
    if _worker_thread is not None and _worker_thread.is_alive():
        _worker_thread.join(timeout=_WORKER_SHUTDOWN_JOIN)
    try:
        _do_logout()
    except Exception:
        pass


atexit.register(_shutdown)
