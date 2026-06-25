"""
硬超时看门狗。

baostock 库的 socketutil.send_msg 在服务端断连后会陷入 recv→b"" 死循环
（recv 对已关闭的 socket 立即返回空 bytes，不阻塞、不触发 socket 超时），
worker 线程 100% CPU 空转、永不返回。watchdog 在 baostock 调用外层设置
硬超时，到点后通过 ctypes 向线程注入 WatchdogTimeout 异常，打断死循环。

原理：recv()/send() 等 syscall 会释放 GIL，PyThreadState_SetAsyncExc 在
目标线程下次获取 GIL 时送达异常。对本场景（recv 高频返回 b""）可靠。
"""

from __future__ import annotations

import ctypes
import logging
import threading

logger = logging.getLogger(__name__)


class WatchdogTimeout(Exception):
    """watchdog 硬超时：baostock 调用未在限定时间内返回，强制中断。

    与 DataSourceError 平级（非子类）：超时不是数据源逻辑错误，而是基础设施
    层面的卡死，worker 需独立处理（重置连接、不重试当前 job）。
    """


def _async_raise(tid: int, exc_type: type) -> None:
    """向指定线程注入异常（CPython C API）。

    目标线程下次获取 GIL 时抛出指定异常。syscall（recv/send/read 等）释放
    GEL 后重获时异常送达；纯 Python 计算则在下一条字节码指令处送达。

    Args:
        tid: 目标线程 ID（threading.get_ident() / Thread.ident 返回值）。
        exc_type: 要注入的异常类。
    """
    if tid <= 0:
        raise ValueError(f"无效线程 ID: {tid}")
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_ulong(tid), ctypes.py_object(exc_type)
    )
    if res == 0:
        raise ValueError(f"线程 {tid} 不存在或已退出")
    if res > 1:
        # tid 匹配了多个线程状态（不应发生），撤销以避免误伤
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(tid), None)
        raise SystemError(f"PyThreadState_SetAsyncExc 返回 {res}，已撤销")


class Watchdog:
    """硬超时上下文管理器。

    用法::

        with Watchdog(threading.get_ident(), timeout=600):
            result = some_blocking_call()

    超时后向目标线程注入 WatchdogTimeout，被 with 块外的 except 捕获。
    正常退出时 cancel timer，无副作用。timer 线程设为 daemon，进程退出时
    自动回收。
    """

    def __init__(self, tid: int, timeout: float) -> None:
        self._tid = tid
        self._timeout = timeout
        self._timer: threading.Timer | None = None

    def _fire(self) -> None:
        logger.warning(
            "watchdog 触发：线程 %s 执行超时（%.0fs），注入 %s 异常",
            self._tid, self._timeout, WatchdogTimeout.__name__,
        )
        try:
            _async_raise(self._tid, WatchdogTimeout)
        except (ValueError, SystemError):
            # 线程已退出或异常注入失败（with 块已结束、timer 竞态），忽略
            logger.debug("watchdog 异常注入未生效（线程可能已退出）")

    def __enter__(self) -> "Watchdog":
        self._timer = threading.Timer(self._timeout, self._fire)
        self._timer.daemon = True
        self._timer.start()
        return self

    def __exit__(self, *exc_info: object) -> bool:
        if self._timer is not None:
            self._timer.cancel()
        return False  # 不吞任何异常（包括 WatchdogTimeout），交给调用方处理
