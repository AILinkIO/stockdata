"""元数据表：数据水位与抓取任务追踪。"""

from datetime import date, datetime
from typing import Any

from sqlalchemy import TIMESTAMP, BigInteger, Date, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class DataWatermark(Base):
    """数据水位：每个 (code, 数据类型) 的覆盖区间与抓取时刻（设计文档 5.2.2）。

    双水位：last_date 是业务水位（数据覆盖到哪天），last_fetched_at 是系统水位
    （最后一次抓取动作的时刻），新鲜度判断见 db/coverage.py。
    覆盖范围是连续闭区间 [first_date, last_date]——首次触达全量回填策略保证无空洞。
    """

    __tablename__ = "data_watermark"

    code: Mapped[str] = mapped_column(
        String(12), primary_key=True, default="", server_default=""
    )  # 全市场数据集用空串
    data_type: Mapped[str] = mapped_column(String(24), primary_key=True)  # DataType

    first_date: Mapped[date | None] = mapped_column(Date)
    last_date: Mapped[date] = mapped_column(Date, nullable=False)
    last_fetched_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )


class FetchTask(Base):
    """抓取任务追踪：观测与去重（部分唯一索引在迁移中手写）。"""

    __tablename__ = "fetch_task"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    task_type: Mapped[str] = mapped_column(String(32), nullable=False)
    params: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    params_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(12), nullable=False, default="pending", server_default="pending"
    )  # TaskStatus
    celery_task_id: Mapped[str | None] = mapped_column(String(64))
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
