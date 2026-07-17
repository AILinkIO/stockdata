"""测试夹具：PG 集成测试库（stockdata_e2e）+ FakeProvider。

集成测试用独立库 stockdata_e2e（不碰生产 stockdata 库）：
DSN 取 STOCKDATA_TEST_PG_DSN，缺省时把 settings.pg_conninfo 的库名换成 stockdata_e2e。
连不上则跳过所有标记 pg 的测试。
"""

from __future__ import annotations

import os

import psycopg
import pytest

from stockdata.config import settings


def _test_dsn() -> str:
    dsn = os.getenv("STOCKDATA_TEST_PG_DSN")
    if dsn:
        return dsn
    base = settings.pg_conninfo
    return base.rsplit("/", 1)[0] + "/stockdata_e2e"


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    dsn = _test_dsn()
    try:
        with psycopg.connect(dsn, connect_timeout=3):
            pass
    except psycopg.OperationalError as e:
        pytest.skip(f"测试 PG 不可用（{e}）")
    return dsn


@pytest.fixture
def pg_db(pg_dsn: str) -> str:
    """每个测试拿到一个已重置并初始化好 schema 的库，返回 DSN。"""
    from stockdata.db.init import init_schema, reset_db

    reset_db(pg_dsn)
    init_schema(pg_dsn)
    return pg_dsn
