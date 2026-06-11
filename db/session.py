"""
数据库引擎与会话工厂。

双引擎设计（见设计文档第 3 章）：
- sync_engine  — fetcher 子进程内使用（Celery 任务是同步上下文）
- async_engine — FastAPI 侧使用

psycopg3 同一 DSN 同时支持两种模式。引擎惰性连接，import 本模块不产生连接。
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from settings import settings

sync_engine = create_engine(settings.pg_dsn, pool_pre_ping=True, pool_size=2)
SyncSession = sessionmaker(sync_engine)

async_engine = create_async_engine(settings.pg_dsn, pool_pre_ping=True)
AsyncSession = async_sessionmaker(async_engine, expire_on_commit=False)
