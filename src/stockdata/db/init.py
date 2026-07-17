"""schema 创建与重置。

- init_schema：执行 schema.sql（全幂等），可重复运行。
- reset_db：drop public schema 下全部表（含旧 dotnet EF 表），随后需重新 init。
"""

from __future__ import annotations

import logging
from importlib import resources

import psycopg

logger = logging.getLogger(__name__)


def _schema_sql() -> str:
    return (resources.files("stockdata.db") / "schema.sql").read_text(encoding="utf-8")


def init_schema(conninfo: str) -> None:
    with psycopg.connect(conninfo) as conn:
        conn.execute(_schema_sql())
    logger.info("schema 初始化完成（幂等）")


def list_tables(conninfo: str) -> list[str]:
    with psycopg.connect(conninfo) as conn:
        rows = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        ).fetchall()
    return [r[0] for r in rows]


def reset_db(conninfo: str) -> list[str]:
    """drop public 下全部表，返回被删除的表名列表。"""
    tables = list_tables(conninfo)
    if not tables:
        return []
    with psycopg.connect(conninfo) as conn:
        for t in tables:
            conn.execute(f'DROP TABLE IF EXISTS "{t}" CASCADE')
    logger.info("已删除 %d 张表: %s", len(tables), ", ".join(tables))
    return tables
