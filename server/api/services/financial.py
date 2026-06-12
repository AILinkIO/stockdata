"""财报读取：单季度各类指标、综合指标合并、快报/预告。"""

from datetime import date

from sqlalchemy import select

from api.services.readthrough import ensure_quarter, ensure_range
from db.coverage import quarter_end
from db.models import FinancialReport
from db.session import SyncSession

QUARTERLY_TYPES = ("profit", "operation", "growth", "balance", "cash_flow", "dupont")


def get_quarterly(code: str, year: int, quarter: int,
                  report_type: str | None = None) -> list[dict]:
    """单季度财报。report_type 为 None 时返回全部六类。"""
    ensure_quarter(code, year, quarter)
    types = [report_type] if report_type else list(QUARTERLY_TYPES)
    with SyncSession() as s:
        rows = s.scalars(
            select(FinancialReport)
            .where(FinancialReport.code == code,
                   FinancialReport.report_type.in_(types),
                   FinancialReport.stat_date == quarter_end(year, quarter))
            .order_by(FinancialReport.report_type)
        ).all()
    return [
        {
            "code": r.code,
            "report_type": r.report_type,
            "stat_date": r.stat_date,
            "pub_date": r.pub_date,
            "metrics": r.metrics,
        }
        for r in rows
    ]


def get_indicator(code: str, start: date, end: date) -> list[dict]:
    """综合财务指标：六类指标按报告期合并为一行，字段加类别前缀（取代旧 get_fina_indicator）。"""
    records = []
    for year in range(start.year, end.year + 1):
        for quarter in range(1, 5):
            q_start = date(year, (quarter - 1) * 3 + 1, 1)
            if q_start > end or quarter_end(year, quarter) < start:
                continue
            rows = get_quarterly(code, year, quarter)
            if not rows:
                continue
            record: dict = {"code": code, "year": year, "quarter": quarter}
            for r in rows:
                for k, v in r["metrics"].items():
                    record[f"{r['report_type']}_{k}"] = v
            if len(record) > 3:
                records.append(record)
    return records


def get_performance(code: str, start: date, end: date, report_type: str) -> list[dict]:
    """业绩快报（express）/ 业绩预告（forecast）。

    日期范围过滤的是披露日期 pub_date（与 baostock 查询语义一致：
    报告期在范围外但披露日在范围内的记录也应返回）。
    """
    ensure_range(report_type, start, end, code)
    with SyncSession() as s:
        rows = s.scalars(
            select(FinancialReport)
            .where(FinancialReport.code == code,
                   FinancialReport.report_type == report_type,
                   FinancialReport.pub_date >= start,
                   FinancialReport.pub_date <= end)
            .order_by(FinancialReport.stat_date)
        ).all()
    return [
        {
            "code": r.code,
            "stat_date": r.stat_date,
            "pub_date": r.pub_date,
            **r.metrics,
        }
        for r in rows
    ]
