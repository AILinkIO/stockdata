"""进程内滑动窗口限流器（自旧 server/core/ratelimit.py 移植，删除 Redis 后端）。

单进程单线程同步引擎唯一需要的实现：deque 记录调用时间戳，
超过窗口的头部弹出的瞬间腾出额度。acquire() 在额度耗尽时阻塞。
max_calls <= 0 时关闭限流。
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque

logger = logging.getLogger(__name__)


class MemoryRateLimiter:
    """进程内滑动窗口限流器（线程安全）。"""

    def __init__(self, max_calls: int, period: float = 60.0) -> None:
        self.max_calls = max_calls
        self.period = period
        self.total_acquired = 0  # 进程累计放行次数（进度展示用）
        self._calls: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        self.total_acquired += 1
        if self.max_calls <= 0:
            return
        while True:
            with self._lock:
                now = time.monotonic()
                while self._calls and now - self._calls[0] >= self.period:
                    self._calls.popleft()
                if len(self._calls) < self.max_calls:
                    self._calls.append(now)
                    return
                sleep_for = self.period - (now - self._calls[0])
            if sleep_for > 0:
                logger.info(
                    "限流：达到 %d 次/%.0fs 配额，等待 %.2fs",
                    self.max_calls, self.period, sleep_for,
                )
                time.sleep(sleep_for)

    def try_acquire(self) -> bool:
        if self.max_calls <= 0:
            return True
        with self._lock:
            now = time.monotonic()
            while self._calls and now - self._calls[0] >= self.period:
                self._calls.popleft()
            if len(self._calls) < self.max_calls:
                self._calls.append(now)
                return True
            return False

    def current_rate(self) -> int:
        """当前窗口内已发生的调用数（供进度展示 calls/min）。"""
        with self._lock:
            now = time.monotonic()
            while self._calls and now - self._calls[0] >= self.period:
                self._calls.popleft()
            return len(self._calls)
