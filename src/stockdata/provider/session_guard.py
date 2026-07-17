"""baostock 登录间隔守卫：任何两次 bs.login() 间隔必须 ≥ min_login_interval_seconds。

时间戳持久化在 PG 单行表 baostock_session，跨进程重启依然生效——开发/测试期
频繁重启进程不会触发频繁 login（真正的红线是 login 频率，不是进程生命周期）。

登录**尝试前**就盖时间戳：失败的 login 同样消耗了服务端的一次连接尝试。
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Callable, Protocol

import psycopg

logger = logging.getLogger(__name__)


class SessionStore(Protocol):
    """登录时间戳的持久化后端（PG 实现 + 测试内存实现）。"""

    def last_login_at(self) -> datetime | None: ...

    def stamp_login(self, at: datetime) -> None: ...


class PgSessionStore:
    """PG 后端：baostock_session 单行表。每次读写用短连接（登录本身低频）。"""

    def __init__(self, conninfo: str) -> None:
        self._conninfo = conninfo

    def last_login_at(self) -> datetime | None:
        with psycopg.connect(self._conninfo) as conn:
            row = conn.execute("SELECT last_login_at FROM baostock_session WHERE id").fetchone()
            return row[0] if row else None

    def stamp_login(self, at: datetime) -> None:
        with psycopg.connect(self._conninfo) as conn:
            conn.execute("UPDATE baostock_session SET last_login_at = %s", (at,))


class MemorySessionStore:
    """内存后端（单测/无 PG 场景）。"""

    def __init__(self) -> None:
        self._at: datetime | None = None

    def last_login_at(self) -> datetime | None:
        return self._at

    def stamp_login(self, at: datetime) -> None:
        self._at = at


class SessionGuard:
    """在每次 bs.login() 之前调用 before_login()：不足最小间隔则 sleep 补齐。"""

    def __init__(
        self,
        store: SessionStore,
        min_interval_seconds: int,
        *,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._store = store
        self._min_interval = min_interval_seconds
        self._sleep = sleep
        self._now = now

    def before_login(self) -> None:
        last = self._store.last_login_at()
        if last is not None and self._min_interval > 0:
            elapsed = (self._now() - last).total_seconds()
            wait = self._min_interval - elapsed
            if wait > 0:
                logger.warning(
                    "登录间隔守卫：距上次 bs.login() 仅 %.0fs（红线 ≥%ds），等待 %.0fs",
                    elapsed, self._min_interval, wait,
                )
                self._sleep(wait)
        # 尝试前盖章：失败的 login 也算一次连接尝试
        self._store.stamp_login(self._now())
