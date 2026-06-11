"""财报表：单表 + 类型 + JSONB（设计文档 5.2.5）。"""

from datetime import date
from typing import Any

from sqlalchemy import Date, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, UpdatedAtMixin


class FinancialReport(UpdatedAtMixin, Base):
    """八类财务数据共用：盈利/营运/成长/偿债/现金流/杜邦/业绩快报/业绩预告。

    stat_date 是报告期（业务时间，主键成分）；pub_date 是披露日期——
    同一报告期重新披露时 stat_date 不变，pub_date 与 metrics 被 upsert 覆盖。
    """

    __tablename__ = "financial_report"

    code: Mapped[str] = mapped_column(String(12), primary_key=True)
    report_type: Mapped[str] = mapped_column(String(20), primary_key=True)  # ReportType
    stat_date: Mapped[date] = mapped_column(Date, primary_key=True)  # 报告期

    pub_date: Mapped[date | None] = mapped_column(Date)  # 披露日期
    metrics: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
