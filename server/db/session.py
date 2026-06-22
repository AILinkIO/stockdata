"""
数据库引擎与会话工厂。

单一同步引擎：同步路由（FastAPI 自动调度到线程池）、读穿透等待、嵌入式 Celery
worker 与 beat 均通过 SyncSession 访问 PostgreSQL。引擎惰性连接，import 本模块
不产生连接。
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from settings import settings

sync_engine = create_engine(settings.pg_dsn, pool_pre_ping=True, pool_size=2)
SyncSession = sessionmaker(sync_engine)
