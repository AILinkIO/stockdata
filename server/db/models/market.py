"""市场概览类表：基本信息、交易日历、股票列表快照、指数成分、行业分类。"""

from datetime import date

from sqlalchemy import Boolean, Date, SmallInteger, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, UpdatedAtMixin


class StockBasic(UpdatedAtMixin, Base):
    """股票基本信息（query_stock_basic）。"""

    __tablename__ = "stock_basic"

    code: Mapped[str] = mapped_column(String(12), primary_key=True)
    code_name: Mapped[str | None] = mapped_column(String(64))
    ipo_date: Mapped[date | None] = mapped_column(Date)
    out_date: Mapped[date | None] = mapped_column(Date)
    type: Mapped[int | None] = mapped_column(SmallInteger)  # 1 股票 / 2 指数 / 3 其它
    status: Mapped[int | None] = mapped_column(SmallInteger)  # 1 上市 / 0 退市


class TradeCalendar(UpdatedAtMixin, Base):
    __tablename__ = "trade_calendar"

    calendar_date: Mapped[date] = mapped_column(Date, primary_key=True)
    is_trading_day: Mapped[bool] = mapped_column(Boolean, nullable=False)


class StockListSnapshot(UpdatedAtMixin, Base):
    """全部股票列表的日期快照（query_all_stock，按需抓取）。"""

    __tablename__ = "stock_list_snapshot"

    snap_date: Mapped[date] = mapped_column(Date, primary_key=True)
    code: Mapped[str] = mapped_column(String(12), primary_key=True)
    code_name: Mapped[str | None] = mapped_column(String(64))
    trade_status: Mapped[bool | None] = mapped_column(Boolean)


class IndexConstituent(UpdatedAtMixin, Base):
    """指数成分股（sz50 / hs300 / zz500）。"""

    __tablename__ = "index_constituent"

    index_code: Mapped[str] = mapped_column(String(8), primary_key=True)  # IndexCode
    snap_date: Mapped[date] = mapped_column(Date, primary_key=True)  # 查询基准日
    code: Mapped[str] = mapped_column(String(12), primary_key=True)
    code_name: Mapped[str | None] = mapped_column(String(64))


class StockIndustry(UpdatedAtMixin, Base):
    """行业分类。"""

    __tablename__ = "stock_industry"

    snap_date: Mapped[date] = mapped_column(Date, primary_key=True)
    code: Mapped[str] = mapped_column(String(12), primary_key=True)
    code_name: Mapped[str | None] = mapped_column(String(64))
    industry: Mapped[str | None] = mapped_column(String(64))
    industry_classification: Mapped[str | None] = mapped_column(String(64))
