"""除权因子与分红送转。"""

from datetime import date
from typing import Any

from sqlalchemy import Date, Numeric, SmallInteger, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, UpdatedAtMixin


class AdjustFactor(UpdatedAtMixin, Base):
    """除权因子：每个除权除息事件一行（多条方案，见设计文档 5.2.1）。

    新除权事件 = 新增一行；交易所修正历史因子（罕见）= upsert 覆盖。
    复权价计算依赖完整因子序列，读取时与 kline JOIN。
    """

    __tablename__ = "adjust_factor"

    code: Mapped[str] = mapped_column(String(12), primary_key=True)
    divid_operate_date: Mapped[date] = mapped_column(Date, primary_key=True)  # 除权除息日

    fore_adjust_factor: Mapped[float] = mapped_column(Numeric(18, 8), nullable=False)
    back_adjust_factor: Mapped[float] = mapped_column(Numeric(18, 8), nullable=False)
    adjust_factor: Mapped[float | None] = mapped_column(Numeric(18, 8))


class Dividend(UpdatedAtMixin, Base):
    """分红送转（query_dividend_data）。关键日期/比例落类型化列，低频字段进 detail。"""

    __tablename__ = "dividend"

    code: Mapped[str] = mapped_column(String(12), primary_key=True)
    plan_announce_date: Mapped[date] = mapped_column(Date, primary_key=True)  # 预案公告日
    year_type: Mapped[str] = mapped_column(String(7), primary_key=True)  # report/operate

    year: Mapped[int] = mapped_column(SmallInteger, nullable=False)  # 查询归属年份
    regist_date: Mapped[date | None] = mapped_column(Date)  # 股权登记日
    operate_date: Mapped[date | None] = mapped_column(Date)  # 除权除息日
    pay_date: Mapped[date | None] = mapped_column(Date)  # 派息日
    cash_ps_before_tax: Mapped[float | None] = mapped_column(Numeric(12, 6))
    cash_ps_after_tax: Mapped[float | None] = mapped_column(Numeric(12, 6))
    stocks_ps: Mapped[float | None] = mapped_column(Numeric(12, 6))
    reserve_to_stock_ps: Mapped[float | None] = mapped_column(Numeric(12, 6))
    detail: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
