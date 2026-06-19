"""
读穿透编排：水位查询 → coverage 判定 → 缺口抓取 → （调用方再读库）。

ensure_* 系列函数返回后即保证请求范围已覆盖且新鲜，调用方直接 SELECT。
"""

from collections.abc import Iterator
from datetime import date, timedelta

from api.services.dispatch import dispatch_and_wait
from core.timeutil import now_cst as now, today_cst as today
from db import coverage
from db.models import DataWatermark
from db.session import SyncSession


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


# 超大缺口切片上限（天/任务）：单任务时长有界，
# 且每段落库即推进水位——中途失败后重试只补剩余段，全史回填具备断点续传。
# 未列出的数据集（日历/宏观等）行数小，不切。
# adjust_factor 不切片：fetch_adjust_factor 恒整段抓取（fore 序列随新除权全量重算），
# 单次 baostock 调用即返回完整序列，切片只会产生多次冗余的全量抓取。
_SLICE_DAYS = {
    "k_d": 3650, "k_w": 3650, "k_m": 3650,
    "k_5": 730, "k_15": 730, "k_30": 730, "k_60": 730,
}


def _split_range(fs: date, fe: date, max_days: int | None) -> Iterator[tuple[date, date]]:
    """把 [fs, fe] 切成跨度 ≤ max_days 的连续闭区间，升序无缝衔接。"""
    if max_days is None:
        yield fs, fe
        return
    step = timedelta(days=max_days - 1)
    while fs <= fe:
        cut = min(fs + step, fe)
        yield fs, cut
        fs = cut + timedelta(days=1)


def ensure_range(data_type: str, start: date, end: date, code: str = "") -> None:
    decision = coverage.check_range(_watermark(code, data_type), data_type, start, end, now())
    for fs, fe in decision.fetch_ranges:
        for ss, se in _split_range(fs, fe, _SLICE_DAYS.get(data_type)):
            task_name, params = _range_task(data_type, code, ss, se)
            dispatch_and_wait(task_name, params)


def ensure_quarter(code: str, year: int, quarter: int) -> None:
    """六类季度财报：点状覆盖判定（事实表行存在性 + fetch_task 成功记录）。"""
    from sqlalchemy import cast, func, select
    from sqlalchemy.dialects.postgresql import JSONB

    from db.models import FetchTask, FinancialReport, TaskStatus

    stat = coverage.quarter_end(year, quarter)
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
