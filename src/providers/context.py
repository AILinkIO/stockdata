"""
Baostock 登录/登出工具模块。

提供 login_baostock() 上下文管理器，用于在每次 API 调用前后
自动完成 Baostock 的 login/logout，并抑制其向 stdout 输出的登录信息。
"""
import os
import sys
import logging
from contextlib import contextmanager

import baostock as bs

from .interface import LoginError

logger = logging.getLogger(__name__)


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


@contextmanager
def login_baostock():
    """Baostock 登录/登出上下文管理器。

    使用方式::

        with login_baostock():
            rs = bs.query_history_k_data_plus(...)

    登录和登出过程中的 stdout 输出会被抑制，避免干扰 MCP 服务器的 HTTP 响应。
    登录失败时抛出 LoginError。
    """
    with _suppress_stdout():
        lg = bs.login()

    if lg.error_code != '0':
        logger.error(f"Baostock 登录失败: {lg.error_msg}")
        raise LoginError(f"Baostock login failed: {lg.error_msg}")

    logger.debug("Baostock 登录成功")
    try:
        yield
    finally:
        with _suppress_stdout():
            bs.logout()
        logger.debug("Baostock 登出成功")
