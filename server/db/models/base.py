"""SQLAlchemy 声明式基类与公共列类型。"""

from datetime import datetime

from sqlalchemy import TIMESTAMP, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class UpdatedAtMixin:
    """系统时间列：本行最后写入时刻（区别于各表的业务时间列）。"""

    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
