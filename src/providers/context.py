"""
Baostock 会话管理模块。

维护一个持久的 Baostock 登录会话，首次使用时自动登录，进程退出时登出。
遇到会话失效（如"用户未登录"）时可通过 relogin() 重新建立连接。

Baostock 内部维护全局 TCP 连接，不支持并发访问。
通过互斥锁确保同一时间只有一个线程使用 baostock。
"""
import os
import sys
import atexit
import logging
import threading
from contextlib import contextmanager

import baostock as bs

from .interface import LoginError

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_logged_in = False


@contextmanager
def _suppress_stdout():
    """临时将 stdout 重定向到 /dev/null，抑制第三方库的控制台输出。"""
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


def _do_login():
    """执行 baostock 登录，失败时抛出 LoginError。调用方须持有 _lock。"""
    global _logged_in
    with _suppress_stdout():
        lg = bs.login()
    if lg.error_code != '0':
        raise LoginError(f"Baostock login failed: {lg.error_msg}")
    _logged_in = True
    logger.debug("Baostock 登录成功")


@contextmanager
def baostock_session():
    """Baostock 会话上下文管理器。

    使用方式::

        with baostock_session():
            rs = bs.query_history_k_data_plus(...)

    首次调用时自动登录，之后复用会话。通过互斥锁串行化所有 baostock 操作。
    如遇会话失效，在 with 块内调用 relogin() 重新连接。
    """
    with _lock:
        if not _logged_in:
            _do_login()
        yield


def relogin():
    """强制重新登录。必须在 baostock_session() 内调用（已持有锁）。"""
    global _logged_in
    if _logged_in:
        with _suppress_stdout():
            bs.logout()
        _logged_in = False
    _do_login()


def _shutdown():
    """进程退出时登出 baostock。"""
    global _logged_in
    if _logged_in:
        with _suppress_stdout():
            bs.logout()
        _logged_in = False
        logger.debug("Baostock 登出成功")


atexit.register(_shutdown)
