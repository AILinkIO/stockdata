"""K 线表：日/周/月一张表，分钟线独立分区表。只存不复权数据（设计原则 2）。"""

from datetime import date, datetime

from sqlalchemy import TIMESTAMP, BigInteger, Boolean, CHAR, Date, Numeric, SmallInteger, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, UpdatedAtMixin


class Kline(UpdatedAtMixin, Base):
    """日/周/月 K 线。估值与状态字段仅日线有值，周/月为 NULL。"""

    __tablename__ = "kline"

    code: Mapped[str] = mapped_column(String(12), primary_key=True)
    frequency: Mapped[str] = mapped_column(CHAR(1), primary_key=True)  # d / w / m
    trade_date: Mapped[date] = mapped_column(Date, primary_key=True)

    open: Mapped[float | None] = mapped_column(Numeric(12, 4))
    high: Mapped[float | None] = mapped_column(Numeric(12, 4))
    low: Mapped[float | None] = mapped_column(Numeric(12, 4))
    close: Mapped[float | None] = mapped_column(Numeric(12, 4))
    preclose: Mapped[float | None] = mapped_column(Numeric(12, 4))  # 仅日线
    volume: Mapped[int | None] = mapped_column(BigInteger)
    amount: Mapped[float | None] = mapped_column(Numeric(20, 4))
    turn: Mapped[float | None] = mapped_column(Numeric(10, 6))
    pct_chg: Mapped[float | None] = mapped_column(Numeric(10, 6))
    trade_status: Mapped[int | None] = mapped_column(SmallInteger)  # 仅日线
    is_st: Mapped[bool | None] = mapped_column(Boolean)  # 仅日线
    pe_ttm: Mapped[float | None] = mapped_column(Numeric(14, 6))  # 仅日线，估值四件套
    pb_mrq: Mapped[float | None] = mapped_column(Numeric(14, 6))
    ps_ttm: Mapped[float | None] = mapped_column(Numeric(14, 6))
    pcf_ncf_ttm: Mapped[float | None] = mapped_column(Numeric(14, 6))


class KlineMinute(UpdatedAtMixin, Base):
    """分钟 K 线，按 bar_time 年度 RANGE 分区（分区 DDL 在迁移中手写）。"""

    __tablename__ = "kline_minute"
    __table_args__ = {"postgresql_partition_by": "RANGE (bar_time)"}

    code: Mapped[str] = mapped_column(String(12), primary_key=True)
    frequency: Mapped[int] = mapped_column(SmallInteger, primary_key=True)  # 5/15/30/60
    bar_time: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)

    open: Mapped[float | None] = mapped_column(Numeric(12, 4))
    high: Mapped[float | None] = mapped_column(Numeric(12, 4))
    low: Mapped[float | None] = mapped_column(Numeric(12, 4))
    close: Mapped[float | None] = mapped_column(Numeric(12, 4))
    volume: Mapped[int | None] = mapped_column(BigInteger)
    amount: Mapped[float | None] = mapped_column(Numeric(20, 4))
