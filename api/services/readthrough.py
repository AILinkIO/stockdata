"""
读穿透编排：水位查询 → coverage 判定 → 缺口抓取 → （调用方再读库）。

ensure_* 系列函数返回后即保证请求范围已覆盖且新鲜，调用方直接 SELECT。
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

from api.services.dispatch import dispatch_and_wait
from db import coverage
from db.models import DataWatermark
from db.session import SyncSession

_CST = ZoneInfo("Asia/Shanghai")


def now() -> datetime:
    return datetime.now(_CST)


def today() -> date:
    return now().date()


def _watermark(code: str, data_type: str) -> DataWatermark | None:
    with SyncSession() as s:
        return s.get(DataWatermark, {"code": code, "data_type": data_type})


# 范围类数据集 → (任务名, 参数构造)
def _range_task(data_type: str, code: str, fs: date, fe: date) -> tuple[str, dict]:
    iso_s, iso_e = fs.isoformat(), fe.isoformat()
    if data_type in ("k_d", "k_w", "k_m"):
        return "fetcher.fetch_kline", {
            "code": code, "start_date": iso_s, "end_date": iso_e,
            "frequency": data_type[2:],
        }
    if data_type in ("k_5", "k_15", "k_30", "k_60"):
        return "fetcher.fetch_kline_minute", {
            "code": code, "start_date": iso_s, "end_date": iso_e,
            "frequency": int(data_type[2:]),
        }
    if data_type == "adjust_factor":
        return "fetcher.fetch_adjust_factor", {
            "code": code, "start_date": iso_s, "end_date": iso_e,
        }
    if data_type in ("express", "forecast"):
        # 单任务同时覆盖快报与预告
        return "fetcher.fetch_performance_report", {
            "code": code, "start_date": iso_s, "end_date": iso_e,
        }
    if data_type == "trade_calendar":
        return "fetcher.fetch_trade_calendar", {"start_date": iso_s, "end_date": iso_e}
    if data_type in ("deposit_rate", "loan_rate", "rrr"):
        return "fetcher.fetch_macro", {
            "kind": data_type, "start_date": iso_s, "end_date": iso_e,
        }
    if data_type == "money_supply_month":
        return "fetcher.fetch_macro", {
            "kind": data_type,
            "start_date": fs.strftime("%Y-%m"), "end_date": fe.strftime("%Y-%m"),
        }
    if data_type == "money_supply_year":
        return "fetcher.fetch_macro", {
            "kind": data_type, "start_date": str(fs.year), "end_date": str(fe.year),
        }
    raise ValueError(f"未知范围数据集: {data_type}")


def ensure_range(data_type: str, start: date, end: date, code: str = "") -> None:
    decision = coverage.check_range(_watermark(code, data_type), data_type, start, end, now())
    for fs, fe in decision.fetch_ranges:
        task_name, params = _range_task(data_type, code, fs, fe)
        dispatch_and_wait(task_name, params)


def ensure_quarter(code: str, year: int, quarter: int) -> None:
    """六类季度财报：点状覆盖判定（事实表行存在性 + fetch_task 成功记录）。"""
    from sqlalchemy import cast, func, select
    from sqlalchemy.dialects.postgresql import JSONB

    from db.models import FetchTask, FinancialReport, TaskStatus

    m, d = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}[quarter]
    stat = date(year, m, d)
    params = {"code": code, "year": year, "quarter": quarter}
    with SyncSession() as s:
        has_rows = bool(
            s.execute(
                select(func.count())
                .select_from(FinancialReport)
                .where(FinancialReport.code == code, FinancialReport.stat_date == stat)
            ).scalar()
        )
        last_success = s.execute(
            select(func.max(FetchTask.finished_at)).where(
                FetchTask.task_type == "fetcher.fetch_financial_report",
                FetchTask.status == TaskStatus.SUCCEEDED,
                FetchTask.params.op("@>")(cast(params, JSONB)),
            )
        ).scalar()

    decision = coverage.check_quarter(has_rows, last_success, year, quarter, now())
    if not decision.fresh:
        dispatch_and_wait("fetcher.fetch_financial_report", params)


def ensure_dividend(code: str, year: int, year_type: str) -> None:
    decision = coverage.check_range(
        _watermark(code, "dividend"), "dividend",
        date(year, 1, 1), min(date(year, 12, 31), today()), now(),
    )
    if not decision.fresh:
        dispatch_and_wait(
            "fetcher.fetch_dividend",
            {"code": code, "year": year, "year_type": year_type},
        )


_SNAPSHOT_TASKS = {
    "stock_list": ("fetcher.fetch_stock_list", lambda d: {"snap_date": d.isoformat()}),
    "industry": ("fetcher.fetch_industry", lambda d: {"snap_date": d.isoformat()}),
    "index_sz50": ("fetcher.fetch_index_constituent",
                   lambda d: {"index_code": "sz50", "snap_date": d.isoformat()}),
    "index_hs300": ("fetcher.fetch_index_constituent",
                    lambda d: {"index_code": "hs300", "snap_date": d.isoformat()}),
    "index_zz500": ("fetcher.fetch_index_constituent",
                    lambda d: {"index_code": "zz500", "snap_date": d.isoformat()}),
}


def ensure_snapshot(data_type: str, snap_date: date, has_rows: bool) -> None:
    decision = coverage.check_snapshot(
        _watermark("", data_type), data_type, snap_date, has_rows, now()
    )
    if not decision.fresh:
        task_name, build = _SNAPSHOT_TASKS[data_type]
        dispatch_and_wait(task_name, build(snap_date))


def ensure_stock_basic(code: str, has_rows: bool) -> None:
    decision = coverage.check_snapshot(
        _watermark(code, "stock_basic"), "stock_basic", today(), has_rows, now()
    )
    if not decision.fresh:
        dispatch_and_wait("fetcher.fetch_stock_basic", {"code": code})
