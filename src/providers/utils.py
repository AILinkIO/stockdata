"""
Baostock 登录/登出工具模块。

提供 baostock_login_context() 上下文管理器，用于在每次 API 调用前后
自动完成 Baostock 的 login/logout，并抑制其向 stdout 输出的登录信息。
"""
import baostock as bs
import os
import sys
import logging
from contextlib import contextmanager
from .interface import LoginError

logger = logging.getLogger(__name__)


@contextmanager
def baostock_login_context():
    """Baostock 登录/登出上下文管理器。

    使用方式:
        with baostock_login_context():
            # 在此执行 Baostock API 调用
            rs = bs.query_history_k_data_plus(...)

    登录和登出过程中的 stdout 输出会被抑制（重定向到 /dev/null），
    避免干扰 MCP 服务器的 HTTP 响应。登录失败时抛出 LoginError。
    """
    # 保存原始 stdout 文件描述符，将其重定向到 /dev/null 以抑制 Baostock 输出
    original_stdout_fd = sys.stdout.fileno()
    saved_stdout_fd = os.dup(original_stdout_fd)
    devnull_fd = os.open(os.devnull, os.O_WRONLY)

    os.dup2(devnull_fd, original_stdout_fd)
    os.close(devnull_fd)

    logger.debug("正在尝试 Baostock 登录...")
    lg = bs.login()
    logger.debug(f"登录结果: code={lg.error_code}, msg={lg.error_msg}")

    # 恢复 stdout
    os.dup2(saved_stdout_fd, original_stdout_fd)
    os.close(saved_stdout_fd)

    if lg.error_code != '0':
        logger.error(f"Baostock 登录失败: {lg.error_msg}")
        raise LoginError(f"Baostock login failed: {lg.error_msg}")

    logger.info("Baostock 登录成功。")
    try:
        yield  # 在此处执行 Baostock API 调用
    finally:
        # 登出时同样抑制 stdout 输出
        original_stdout_fd = sys.stdout.fileno()
        saved_stdout_fd = os.dup(original_stdout_fd)
        devnull_fd = os.open(os.devnull, os.O_WRONLY)

        os.dup2(devnull_fd, original_stdout_fd)
        os.close(devnull_fd)

        logger.debug("正在尝试 Baostock 登出...")
        bs.logout()
        logger.debug("登出完成。")

        # 恢复 stdout
        os.dup2(saved_stdout_fd, original_stdout_fd)
        os.close(saved_stdout_fd)
        logger.info("Baostock 登出成功。")
