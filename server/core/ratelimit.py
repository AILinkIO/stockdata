"""
通用限流模块。

滑动窗口算法，两种实现：
- MemoryRateLimiter：进程内 deque + threading.Lock，零外部依赖，适合单进程。
- RedisRateLimiter：Redis ZSET + Lua 原子操作，跨进程安全，适合多实例。

通过 create_rate_limiter() 工厂按配置选择实现。
"""

from __future__ import annotations

import abc
import logging
import threading
import time
import uuid
from collections import deque

logger = logging.getLogger(__name__)


class BaseRateLimiter(abc.ABC):
    """限流器抽象基类。

    滑动窗口语义：窗口内允许突发到 max_calls，之后节流。
    max_calls <= 0 时关闭限流（所有方法直接通过）。
    """

    max_calls: int
    period: float

    @abc.abstractmethod
    def acquire(self) -> None:
        ...

    @abc.abstractmethod
    def try_acquire(self) -> bool:
        ...

    def close(self) -> None:
        """释放底层资源（如 Redis 连接）。MemoryRateLimiter 无需实现。"""


class MemoryRateLimiter(BaseRateLimiter):
    """进程内滑动窗口限流器。

    deque 记录每次调用的时间戳，超过窗口的头部弹出的瞬间腾出额度。
    acquire() 在额度耗尽时阻塞到最早调用滑出窗口。自带锁，线程安全。
    """

    def __init__(self, max_calls: int, period: float = 60.0) -> None:
        self.max_calls = max_calls
        self.period = period
        self._calls: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
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


# Lua 原子滑动窗口：ZREMRANGEBYSCORE 清过期 → ZCARD 计数 → 未满则 ZADD 入队，
# 满了则返回需等待的毫秒数（最早成员 score + window - now）。
_LUA_SLIDING_WINDOW = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local max_calls = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window_ms)

local count = redis.call('ZCARD', key)

if count < max_calls then
    redis.call('ZADD', key, now, member)
    redis.call('EXPIRE', key, math.ceil(window_ms / 1000) + 1)
    return 0
else
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local oldest_score = tonumber(oldest[2])
    return oldest_score + window_ms - now
end
"""


class RedisRateLimiter(BaseRateLimiter):
    """Redis 滑动窗口限流器（Lua 原子操作，跨进程安全）。

    所有实例共享同一个 Redis key，通过 ZSET score（毫秒时间戳）实现全局滑动窗口。
    acquire() 在额度耗尽时 sleep 返回的等待时长后重试。
    """

    def __init__(
        self,
        redis_url: str,
        key: str,
        max_calls: int,
        period: float = 60.0,
    ) -> None:
        import redis

        self.max_calls = max_calls
        self.period = period
        self._key = key
        self._redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self._script = self._redis.register_script(_LUA_SLIDING_WINDOW)

    def acquire(self) -> None:
        if self.max_calls <= 0:
            return
        while True:
            wait_ms = self._eval()
            if wait_ms == 0:
                return
            sleep_for = wait_ms / 1000.0
            logger.info(
                "限流（Redis %s）：达到 %d 次/%.0fs 配额，等待 %.2fs",
                self._key, self.max_calls, self.period, sleep_for,
            )
            time.sleep(sleep_for)

    def try_acquire(self) -> bool:
        if self.max_calls <= 0:
            return True
        return self._eval() == 0

    def _eval(self) -> int:
        now_ms = int(time.time() * 1000)
        member = f"{now_ms}:{uuid.uuid4().hex}"
        return int(self._script(
            keys=[self._key],
            args=[now_ms, int(self.period * 1000), self.max_calls, member],
        ))

    def close(self) -> None:
        self._redis.close()


def create_rate_limiter(
    max_calls: int,
    period: float = 60.0,
    *,
    backend: str = "memory",
    redis_url: str = "",
    key: str = "ratelimit:default",
) -> BaseRateLimiter:
    """工厂函数：根据 backend 创建对应限流器。

    Args:
        max_calls: 窗口内最大调用次数，<=0 关闭限流。
        period: 窗口大小（秒），默认 60。
        backend: "memory" 或 "redis"。
        redis_url: Redis 连接地址（backend="redis" 时必填）。
        key: Redis ZSET key（backend="redis" 时使用）。
    """
    if backend == "redis":
        if not redis_url:
            raise ValueError("backend='redis' 时必须提供 redis_url")
        return RedisRateLimiter(redis_url, key, max_calls, period)

    return MemoryRateLimiter(max_calls, period)
