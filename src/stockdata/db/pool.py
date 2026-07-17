"""PG 连接：进程级惰性单例连接池（web 读 + 同步引擎写共用）。"""

from __future__ import annotations

import threading

import psycopg
from psycopg_pool import ConnectionPool

from stockdata.config import settings

_pool: ConnectionPool | None = None
_lock = threading.Lock()


def get_pool(conninfo: str | None = None) -> ConnectionPool:
    global _pool
    if _pool is None:
        with _lock:
            if _pool is None:
                _pool = ConnectionPool(
                    conninfo or settings.pg_conninfo,
                    min_size=1,
                    max_size=4,
                    open=True,
                )
    return _pool


def connect(conninfo: str | None = None) -> psycopg.Connection:
    """独立短连接（db init/reset、SessionStore 等低频场景）。"""
    return psycopg.connect(conninfo or settings.pg_conninfo)
