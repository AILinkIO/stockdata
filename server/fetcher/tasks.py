"""
fetcher 任务定义（设计文档 4.2 节）。

统一骨架：标记 fetch_task.running → provider 查询 → writer 落库（与水位更新同事务）
→ 标记 succeeded/failed。

约定：
- NoDataFoundError 对范围/列表类查询是合法的 0 行结果：**定型区**照常更新水位
  （声明"该范围已检查过，没有数据"），防止读穿透反复触发抓取；**未定型尾部**
  只声明实际返回的数据（db/coverage.claimable_last），数据源尚未发布的日期
  留待后续重抓，避免形成永久空洞。
- DataSourceError 自动重试（退避，最多 2 次）；重试耗尽才标记 failed。
- 所有日期参数为 'YYYY-MM-DD' 字符串（货币供应量为 'YYYY-MM' / 'YYYY'）。
- 快照类任务（股票列表/成分股/行业）的 snap_date 由调用方解析为具体交易日。
"""

import logging
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from billiard.exceptions import SoftTimeLimitExceeded
from sqlalchemy import update as sa_update

from db.coverage import claimable_last
from db.models import DataType, FetchTask, TaskStatus
from db.session import SyncSession
from fetcher import writer
from fetcher.app import app
from fetcher.providers import baostock as provider
from fetcher.providers.interface import DataSourceError, NoDataFoundError

logger = logging.getLogger(__name__)

_CST = ZoneInfo("Asia/Shanghai")


def _today() -> date:
    return datetime.now(_CST).date()


def _d(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _max_date_str(df, col: str) -> date | None:
    """DataFrame 中 'YYYY-MM-DD' 字符串列的最大日期（字典序即日期序），无数据返回 None。"""
    if df is None or len(df) == 0 or col not in df:
        return None
    vals = [str(v) for v in df[col].tolist() if v]
    return _d(max(vals)) if vals else None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _mark(fetch_task_id: int | None, **fields) -> None:
    """更新 fetch_task 追踪行（独立短事务，不与数据写入耦合）。"""
    if fetch_task_id is None:
        return
    try:
        with SyncSession.begin() as s:
            s.execute(
                sa_update(FetchTask).where(FetchTask.id == fetch_task_id).values(**fields)
            )
    except Exception:
        logger.exception("更新 fetch_task #%s 状态失败", fetch_task_id)


_TASK_OPTS = dict(
    bind=True,
    # SoftTimeLimitExceeded 也任务级重试：进程内剩余预算不足时 provider 直接
    # 放行该异常，重新投递的执行有全新时限
    autoretry_for=(DataSourceError, SoftTimeLimitExceeded),
    dont_autoretry_for=(NoDataFoundError,),
    retry_backoff=5,
    retry_backoff_max=60,
    max_retries=2,
)


def _run(task, fetch_task_id: int | None, impl) -> dict:
    """任务统一骨架：状态标记 + 重试耗尽才 failed。"""
    _mark(
        fetch_task_id,
        status=TaskStatus.RUNNING,
        started_at=_now(),
        celery_task_id=task.request.id,
    )
    try:
        result = impl()
    except Exception as e:
        if task.request.retries >= task.max_retries:  # 重试耗尽
            _mark(fetch_task_id, status=TaskStatus.FAILED, error=str(e), finished_at=_now())
        raise
    _mark(fetch_task_id, status=TaskStatus.SUCCEEDED, finished_at=_now())
    return result


# ── K 线 ──


@app.task(name="fetcher.fetch_kline", **_TASK_OPTS)
def fetch_kline(self, code: str, start_date: str, end_date: str,
                frequency: str = "d", fetch_task_id: int | None = None) -> dict:
    def impl() -> dict:
        try:
            df = provider.query_k_data(code, start_date, end_date, frequency)
        except NoDataFoundError:
            df = None
        data_type = DataType.from_k_frequency(frequency)
        with SyncSession.begin() as s:
            n = writer.write_kline(s, df, code, frequency) if df is not None else 0
            writer.update_watermark(
                s, data_type,
                last_date=claimable_last(
                    data_type, _d(end_date), _max_date_str(df, "date"), _today()
                ),
                first_date=_d(start_date), code=code,
            )
        return {"rows": n}

    return _run(self, fetch_task_id, impl)


@app.task(name="fetcher.fetch_kline_minute", **_TASK_OPTS)
def fetch_kline_minute(self, code: str, start_date: str, end_date: str,
                       frequency: int = 30, fetch_task_id: int | None = None) -> dict:
    def impl() -> dict:
        try:
            df = provider.query_k_data(code, start_date, end_date, str(frequency))
        except NoDataFoundError:
            df = None
        # 分钟线 time 列为 YYYYMMDDHHMMSSsss，取前 8 位作业务日期
        actual_last = None
        if df is not None and len(df) and "time" in df:
            vals = [str(v) for v in df["time"].tolist() if v]
            if vals:
                actual_last = datetime.strptime(max(vals)[:8], "%Y%m%d").date()
        data_type = DataType.from_k_frequency(str(frequency))
        with SyncSession.begin() as s:
            n = writer.write_kline_minute(s, df, code, frequency) if df is not None else 0
            writer.update_watermark(
                s, data_type,
                last_date=claimable_last(data_type, _d(end_date), actual_last, _today()),
                first_date=_d(start_date), code=code,
            )
        return {"rows": n}

    return _run(self, fetch_task_id, impl)


# ── 除权因子 / 基本信息 / 分红 ──


@app.task(name="fetcher.fetch_adjust_factor", **_TASK_OPTS)
def fetch_adjust_factor(self, code: str, start_date: str, end_date: str,
                        fetch_task_id: int | None = None) -> dict:
    def impl() -> dict:
        try:
            df = provider.query_adjust_factor(code, start_date, end_date)
        except NoDataFoundError:
            df = None  # 从未除权是合法状态
        with SyncSession.begin() as s:
            n = writer.write_adjust_factor(s, df, code) if df is not None else 0
            writer.update_watermark(
                s, DataType.ADJUST_FACTOR,
                last_date=claimable_last(
                    DataType.ADJUST_FACTOR, _d(end_date),
                    _max_date_str(df, "dividOperateDate"), _today(),
                ),
                first_date=_d(start_date), code=code,
            )
        return {"rows": n}

    return _run(self, fetch_task_id, impl)


@app.task(name="fetcher.fetch_stock_basic", **_TASK_OPTS)
def fetch_stock_basic(self, code: str, fetch_task_id: int | None = None) -> dict:
    def impl() -> dict:
        df = provider.query_stock_basic(code)
        with SyncSession.begin() as s:
            n = writer.write_stock_basic(s, df)
            writer.update_watermark(s, DataType.STOCK_BASIC, last_date=_today(), code=code)
        return {"rows": n}

    return _run(self, fetch_task_id, impl)


@app.task(name="fetcher.fetch_dividend", **_TASK_OPTS)
def fetch_dividend(self, code: str, year: int, year_type: str = "report",
                   fetch_task_id: int | None = None) -> dict:
    def impl() -> dict:
        try:
            df = provider.query_dividend(code, str(year), year_type)
        except NoDataFoundError:
            df = None  # 该年无分红是合法状态
        with SyncSession.begin() as s:
            n = writer.write_dividend(s, df, code, year, year_type) if df is not None else 0
            writer.update_watermark(
                s, DataType.DIVIDEND,
                last_date=min(date(year, 12, 31), _today()),
                first_date=date(year, 1, 1), code=code,
            )
        return {"rows": n}

    return _run(self, fetch_task_id, impl)


# ── 财报 ──

_QUARTER_END = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}


@app.task(name="fetcher.fetch_financial_report", **_TASK_OPTS)
def fetch_financial_report(self, code: str, year: int, quarter: int,
                           fetch_task_id: int | None = None) -> dict:
    """单季度六类财务数据（盈利/营运/成长/偿债/现金流/杜邦）。"""

    def impl() -> dict:
        categories = provider.query_fina_quarter(code, str(year), quarter)
        total = 0
        with SyncSession.begin() as s:
            for report_type, rec in categories.items():
                total += writer.write_financial_reports(s, code, report_type, [rec])
        # 不写水位：季度覆盖是点状语义，由 fetch_task 成功记录承担
        # "已查过"记忆（含空结果），区间水位会虚假覆盖中间未抓取的季度
        return {"rows": total, "categories": sorted(categories)}

    return _run(self, fetch_task_id, impl)


@app.task(name="fetcher.fetch_performance_report", **_TASK_OPTS)
def fetch_performance_report(self, code: str, start_date: str, end_date: str,
                             fetch_task_id: int | None = None) -> dict:
    """业绩快报 + 业绩预告（同一日期范围一次抓取）。"""

    def impl() -> dict:
        total = 0
        with SyncSession.begin() as s:
            for report_type, query, stat_key, pub_key in (
                ("express", provider.query_performance_express,
                 "performanceExpStatDate", "performanceExpPubDate"),
                ("forecast", provider.query_forecast,
                 "profitForcastExpStatDate", "profitForcastExpPubDate"),
            ):
                try:
                    df = query(code, start_date, end_date)
                    total += writer.write_financial_reports(
                        s, code, report_type, df.to_dict("records"),
                        stat_key=stat_key, pub_key=pub_key,
                    )
                except NoDataFoundError:
                    pass  # 范围内无快报/预告是常态
                writer.update_watermark(
                    s, report_type,
                    last_date=claimable_last(report_type, _d(end_date), None, _today()),
                    first_date=_d(start_date), code=code,
                )
        return {"rows": total}

    return _run(self, fetch_task_id, impl)


# ── 市场概览 ──


@app.task(name="fetcher.fetch_trade_calendar", **_TASK_OPTS)
def fetch_trade_calendar(self, start_date: str, end_date: str,
                         fetch_task_id: int | None = None) -> dict:
    def impl() -> dict:
        df = provider.query_trade_dates(start_date, end_date)
        with SyncSession.begin() as s:
            n = writer.write_trade_calendar(s, df)
            # 日历可请求未来日期，水位不 clamp 到今天
            writer.update_watermark(
                s, DataType.TRADE_CALENDAR,
                last_date=_d(end_date), first_date=_d(start_date),
            )
        return {"rows": n}

    return _run(self, fetch_task_id, impl)


@app.task(name="fetcher.fetch_stock_list", **_TASK_OPTS)
def fetch_stock_list(self, snap_date: str, fetch_task_id: int | None = None) -> dict:
    def impl() -> dict:
        try:
            df = provider.query_all_stock(snap_date)
        except NoDataFoundError:
            # 当日列表盘中尚未发布是常态：0 行返回，不记水位（API 层回退前一交易日）
            return {"rows": 0}
        with SyncSession.begin() as s:
            n = writer.write_stock_list(s, df, _d(snap_date))
            writer.update_watermark(s, DataType.STOCK_LIST, last_date=_d(snap_date))
        return {"rows": n}

    return _run(self, fetch_task_id, impl)


@app.task(name="fetcher.fetch_index_constituent", **_TASK_OPTS)
def fetch_index_constituent(self, index_code: str, snap_date: str,
                            fetch_task_id: int | None = None) -> dict:
    def impl() -> dict:
        df = provider.query_index_constituent(index_code, snap_date)
        with SyncSession.begin() as s:
            n = writer.write_index_constituent(s, df, index_code, _d(snap_date))
            writer.update_watermark(s, f"index_{index_code}", last_date=_d(snap_date))
        return {"rows": n}

    return _run(self, fetch_task_id, impl)


@app.task(name="fetcher.fetch_industry", **_TASK_OPTS)
def fetch_industry(self, snap_date: str, fetch_task_id: int | None = None) -> dict:
    def impl() -> dict:
        df = provider.query_industry(snap_date)
        with SyncSession.begin() as s:
            n = writer.write_industry(s, df, _d(snap_date))
            writer.update_watermark(s, DataType.INDUSTRY, last_date=_d(snap_date))
        return {"rows": n}

    return _run(self, fetch_task_id, impl)


# ── 宏观 ──


def _macro_dates(kind: str, start_date: str, end_date: str) -> tuple[date, date]:
    """货币供应量参数为 YYYY-MM / YYYY，水位统一折算为 date。"""
    if kind == "money_supply_month":
        return (
            datetime.strptime(start_date, "%Y-%m").date(),
            datetime.strptime(end_date, "%Y-%m").date(),
        )
    if kind == "money_supply_year":
        return date(int(start_date), 1, 1), date(int(end_date), 1, 1)
    return _d(start_date), _d(end_date)


# ── 联调/部署验证 ──


@app.task(name="fetcher.debug_probe", bind=True)
def debug_probe(self, sleep_seconds: int = 0, code: str = "") -> dict:
    """部署验证探针：sleep_seconds>0 时模拟挂死（无视软超时，逼出 SIGKILL 路径）；
    返回子进程 PID 用于观察 max_tasks_per_child 回收与分片亲和（code 参与路由）。
    不触碰 baostock 与数据库。"""
    import os
    import time

    from billiard.exceptions import SoftTimeLimitExceeded

    try:
        time.sleep(sleep_seconds)
    except SoftTimeLimitExceeded:
        time.sleep(sleep_seconds)
    return {"pid": os.getpid(), "code": code}


@app.task(name="fetcher.fetch_macro", **_TASK_OPTS)
def fetch_macro(self, kind: str, start_date: str, end_date: str,
                fetch_task_id: int | None = None) -> dict:
    def impl() -> dict:
        try:
            df = provider.query_macro(kind, start_date, end_date)
        except NoDataFoundError:
            df = None
        first, last = _macro_dates(kind, start_date, end_date)
        with SyncSession.begin() as s:
            n = writer.write_macro(s, df, kind) if df is not None else 0
            writer.update_watermark(
                s, kind, last_date=min(last, _today()), first_date=first
            )
        return {"rows": n}

    return _run(self, fetch_task_id, impl)
