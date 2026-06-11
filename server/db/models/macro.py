"""宏观经济表。列名照 baostock 实际返回字段直接落列（已实测确认），宽表结构。"""

from datetime import date

from sqlalchemy import Date, Numeric, SmallInteger
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, UpdatedAtMixin

_RATE = Numeric(8, 4)       # 利率/比率（%）
_AMOUNT = Numeric(20, 4)    # 货币量（亿元）
_PCT = Numeric(10, 4)       # 同比/环比（%）


class DepositRate(UpdatedAtMixin, Base):
    """基准存款利率（query_deposit_rate_data），每个发布日一行。"""

    __tablename__ = "deposit_rate"

    pub_date: Mapped[date] = mapped_column(Date, primary_key=True)
    demand_deposit_rate: Mapped[float | None] = mapped_column(_RATE)  # 活期
    fixed_deposit_rate_3month: Mapped[float | None] = mapped_column(_RATE)
    fixed_deposit_rate_6month: Mapped[float | None] = mapped_column(_RATE)
    fixed_deposit_rate_1year: Mapped[float | None] = mapped_column(_RATE)
    fixed_deposit_rate_2year: Mapped[float | None] = mapped_column(_RATE)
    fixed_deposit_rate_3year: Mapped[float | None] = mapped_column(_RATE)
    fixed_deposit_rate_5year: Mapped[float | None] = mapped_column(_RATE)
    installment_fixed_deposit_rate_1year: Mapped[float | None] = mapped_column(_RATE)
    installment_fixed_deposit_rate_3year: Mapped[float | None] = mapped_column(_RATE)
    installment_fixed_deposit_rate_5year: Mapped[float | None] = mapped_column(_RATE)


class LoanRate(UpdatedAtMixin, Base):
    """基准贷款利率（query_loan_rate_data），每个发布日一行。"""

    __tablename__ = "loan_rate"

    pub_date: Mapped[date] = mapped_column(Date, primary_key=True)
    loan_rate_6month: Mapped[float | None] = mapped_column(_RATE)
    loan_rate_6month_to_1year: Mapped[float | None] = mapped_column(_RATE)
    loan_rate_1year_to_3year: Mapped[float | None] = mapped_column(_RATE)
    loan_rate_3year_to_5year: Mapped[float | None] = mapped_column(_RATE)
    loan_rate_above_5year: Mapped[float | None] = mapped_column(_RATE)
    mortgage_rate_below_5year: Mapped[float | None] = mapped_column(_RATE)
    mortgage_rate_above_5year: Mapped[float | None] = mapped_column(_RATE)


class RequiredReserveRatio(UpdatedAtMixin, Base):
    """存款准备金率（query_required_reserve_ratio_data）。"""

    __tablename__ = "required_reserve_ratio"

    pub_date: Mapped[date] = mapped_column(Date, primary_key=True)
    effective_date: Mapped[date] = mapped_column(Date, primary_key=True)
    big_institutions_ratio_pre: Mapped[float | None] = mapped_column(_RATE)
    big_institutions_ratio_after: Mapped[float | None] = mapped_column(_RATE)
    medium_institutions_ratio_pre: Mapped[float | None] = mapped_column(_RATE)
    medium_institutions_ratio_after: Mapped[float | None] = mapped_column(_RATE)


class MoneySupplyMonth(UpdatedAtMixin, Base):
    """月度货币供应量（query_money_supply_data_month）。"""

    __tablename__ = "money_supply_month"

    stat_year: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    stat_month: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    m0_month: Mapped[float | None] = mapped_column(_AMOUNT)
    m0_yoy: Mapped[float | None] = mapped_column(_PCT)
    m0_chain_relative: Mapped[float | None] = mapped_column(_PCT)
    m1_month: Mapped[float | None] = mapped_column(_AMOUNT)
    m1_yoy: Mapped[float | None] = mapped_column(_PCT)
    m1_chain_relative: Mapped[float | None] = mapped_column(_PCT)
    m2_month: Mapped[float | None] = mapped_column(_AMOUNT)
    m2_yoy: Mapped[float | None] = mapped_column(_PCT)
    m2_chain_relative: Mapped[float | None] = mapped_column(_PCT)


class MoneySupplyYear(UpdatedAtMixin, Base):
    """年度货币供应量（query_money_supply_data_year，年末余额）。"""

    __tablename__ = "money_supply_year"

    stat_year: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    m0_year: Mapped[float | None] = mapped_column(_AMOUNT)
    m0_year_yoy: Mapped[float | None] = mapped_column(_PCT)
    m1_year: Mapped[float | None] = mapped_column(_AMOUNT)
    m1_year_yoy: Mapped[float | None] = mapped_column(_PCT)
    m2_year: Mapped[float | None] = mapped_column(_AMOUNT)
    m2_year_yoy: Mapped[float | None] = mapped_column(_PCT)
