"""
Baostock 查询函数（自 src/providers/baostock.py 迁入）。

与旧实现的关键差异：**没有线程与队列**。本模块只在 Celery prefork 子进程内使用，
进程即隔离边界——每个子进程在 worker_process_init 时登录，持有独立的 baostock
全局 TCP 连接；挂死由父进程的 task_time_limit SIGKILL 兜底（见设计文档 4.1）。

可重试错误（连接断开/未登录）在进程内重登录一次重试；仍失败则抛出，
交由 Celery 任务级重试或最终失败。
"""

import logging
import os
import sys
import time
from contextlib import contextmanager

import baostock as bs
import pandas as pd

from .interface import DataSourceError, LoginError, NoDataFoundError

logger = logging.getLogger(__name__)

# 需要通过重连重试才能恢复的错误码（与旧 context.py 一致）
_RETRYABLE_CODES = frozenset(
    {
        "10001001",  # 用户未登录
        "10002001",  # 网络错误
        "10002002",  # 网络连接失败
        "10002004",  # 连接断开
        "10002007",  # 网络接收错误
    }
)

_logged_in = False  # 子进程内的登录状态
_last_query_at = 0.0  # 上次成功查询时刻（monotonic）
_IDLE_RELOGIN_SECONDS = 60  # baostock 服务端会掐闲置连接；闲置超过此值预防性重登录


def _is_retryable_error(exc: Exception) -> bool:
    msg = str(exc)
    return (
        any(code in msg for code in _RETRYABLE_CODES)
        or "login" in msg.lower()
        or "未登录" in msg
        or "Broken pipe" in msg
    )


@contextmanager
def _suppress_stdout():
    """屏蔽 baostock login/logout 直接 print 到 stdout 的噪音。

    Celery 子进程中 sys.stdout 是 LoggingProxy（无 fileno），此时退化为
    redirect_stdout；普通进程中沿用 fd 级屏蔽。
    """
    try:
        original_fd = sys.stdout.fileno()
    except (AttributeError, OSError, ValueError):
        import contextlib
        import io

        with contextlib.redirect_stdout(io.StringIO()):
            yield
        return

    saved_fd = os.dup(original_fd)
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull_fd, original_fd)
    os.close(devnull_fd)
    try:
        yield
    finally:
        os.dup2(saved_fd, original_fd)
        os.close(saved_fd)


def ensure_login() -> None:
    """确保当前进程已登录。失败抛 LoginError（任务层会重试/标记失败）。"""
    global _logged_in
    if _logged_in:
        return
    with _suppress_stdout():
        lg = bs.login()
    if lg.error_code != "0":
        raise LoginError(f"Baostock 登录失败: {lg.error_msg} (code: {lg.error_code})")
    _logged_in = True
    logger.info("Baostock 登录成功 (pid=%s)", os.getpid())


def force_relogin() -> None:
    """放弃当前连接直接重新登录。

    故意不调用 bs.logout()：旧连接可能已死，logout 的 recv 会和查询一样
    阻塞挂死；bs.login() 会新建 socket 替换模块级连接，旧会话由服务端回收。
    """
    global _logged_in
    _logged_in = False
    ensure_login()


def logout() -> None:
    global _logged_in
    try:
        with _suppress_stdout():
            bs.logout()
    except Exception:
        pass
    _logged_in = False


# ── 通用查询流程 ──


def _check_api_error(rs, description: str) -> None:
    if rs.error_code == "0":
        return
    msg = f"{description}: {rs.error_msg} (code: {rs.error_code})"
    if "no record found" in rs.error_msg.lower() or rs.error_code == "10002":
        raise NoDataFoundError(msg)
    raise DataSourceError(msg)


def _collect_rows(rs, description: str) -> pd.DataFrame:
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        raise NoDataFoundError(f"{description}: 查询结果为空")
    df = pd.DataFrame(rows, columns=rs.fields)
    logger.info("%s: 获取 %d 条记录", description, len(df))
    return df


def _query(bs_func, description: str, **kwargs) -> pd.DataFrame:
    """调用 API → 校验 → 收集数据；可重试错误重登录后重试一次。

    连接闲置超过 _IDLE_RELOGIN_SECONDS 时预防性重登录：baostock 服务端会
    静默断开闲置连接，复用死连接会阻塞在 recv 直到被超时机制击杀（实测）。
    """
    global _last_query_at
    ensure_login()
    if _last_query_at and time.monotonic() - _last_query_at > _IDLE_RELOGIN_SECONDS:
        logger.info("连接闲置超过 %ds，预防性重新登录", _IDLE_RELOGIN_SECONDS)
        force_relogin()
    logger.info("正在查询 %s", description)

    def _do() -> pd.DataFrame:
        rs = bs_func(**kwargs)
        _check_api_error(rs, description)
        df = _collect_rows(rs, description)
        return df

    try:
        result = _do()
    except NoDataFoundError:
        _last_query_at = time.monotonic()  # 空结果也是有效会话往返
        raise
    except Exception as e:
        if not _is_retryable_error(e):
            if isinstance(e, DataSourceError):
                raise
            raise DataSourceError(f"{description}: 未预期错误 - {e}") from e
        logger.warning("%s: 可重试错误，重新登录后重试一次: %s", description, e)
        force_relogin()
        result = _do()
    _last_query_at = time.monotonic()
    return result


# ── K 线默认字段（按频率区分，与旧实现一致） ──

_DAILY_K_FIELDS = [
    "date", "code", "open", "high", "low", "close", "preclose", "volume",
    "amount", "adjustflag", "turn", "tradestatus", "pctChg",
    "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM", "isST",
]
_WEEKLY_MONTHLY_K_FIELDS = [
    "date", "code", "open", "high", "low", "close", "volume", "amount",
    "adjustflag", "turn", "pctChg",
]
_MINUTE_K_FIELDS = [
    "date", "time", "code", "open", "high", "low", "close", "volume",
    "amount", "adjustflag",
]


def _default_k_fields(frequency: str) -> list[str]:
    if frequency in ("w", "m"):
        return _WEEKLY_MONTHLY_K_FIELDS
    if frequency in ("5", "15", "30", "60"):
        return _MINUTE_K_FIELDS
    return _DAILY_K_FIELDS


# ── 查询函数 ──


def query_k_data(code: str, start_date: str, end_date: str, frequency: str) -> pd.DataFrame:
    """K 线，恒为不复权（adjustflag=3，见设计原则 2：复权读时计算）。"""
    return _query(
        bs.query_history_k_data_plus,
        f"K线 {code} {frequency} {start_date}~{end_date}",
        code=code,
        fields=",".join(_default_k_fields(frequency)),
        start_date=start_date,
        end_date=end_date,
        frequency=frequency,
        adjustflag="3",
    )


def query_adjust_factor(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    return _query(
        bs.query_adjust_factor,
        f"复权因子 {code} {start_date}~{end_date}",
        code=code, start_date=start_date, end_date=end_date,
    )


def query_stock_basic(code: str) -> pd.DataFrame:
    return _query(bs.query_stock_basic, f"基本信息 {code}", code=code)


def query_dividend(code: str, year: str, year_type: str) -> pd.DataFrame:
    return _query(
        bs.query_dividend_data,
        f"分红 {code} {year}",
        code=code, year=year, yearType=year_type,
    )


# 六类季度财报：(bs 查询函数, report_type)
FINA_CATEGORIES = [
    (bs.query_profit_data, "profit"),
    (bs.query_operation_data, "operation"),
    (bs.query_growth_data, "growth"),
    (bs.query_balance_data, "balance"),
    (bs.query_cash_flow_data, "cash_flow"),
    (bs.query_dupont_data, "dupont"),
]


def query_fina_quarter(code: str, year: str, quarter: int) -> dict[str, dict]:
    """单季度六类财务数据。返回 {report_type: {field: value}}，无数据的类别缺席。"""
    ensure_login()
    result: dict[str, dict] = {}
    for bs_func, report_type in FINA_CATEGORIES:
        desc = f"{report_type} {code} {year}Q{quarter}"
        try:
            df = _query(bs_func, desc, code=code, year=year, quarter=quarter)
            result[report_type] = df.iloc[0].to_dict()
        except NoDataFoundError:
            logger.debug("%s: 无数据", desc)
    return result


def query_performance_express(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    return _query(
        bs.query_performance_express_report,
        f"业绩快报 {code} {start_date}~{end_date}",
        code=code, start_date=start_date, end_date=end_date,
    )


def query_forecast(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    return _query(
        bs.query_forecast_report,
        f"业绩预告 {code} {start_date}~{end_date}",
        code=code, start_date=start_date, end_date=end_date,
    )


def query_trade_dates(start_date: str, end_date: str) -> pd.DataFrame:
    return _query(
        bs.query_trade_dates,
        f"交易日历 {start_date}~{end_date}",
        start_date=start_date, end_date=end_date,
    )


def query_all_stock(date: str) -> pd.DataFrame:
    return _query(bs.query_all_stock, f"全部股票 {date}", day=date)


def query_industry(date: str) -> pd.DataFrame:
    return _query(bs.query_stock_industry, f"行业分类 {date}", date=date)


_INDEX_QUERIES = {
    "sz50": bs.query_sz50_stocks,
    "hs300": bs.query_hs300_stocks,
    "zz500": bs.query_zz500_stocks,
}


def query_index_constituent(index_code: str, date: str) -> pd.DataFrame:
    return _query(
        _INDEX_QUERIES[index_code], f"{index_code}成分股 {date}", date=date
    )


_MACRO_QUERIES = {
    "deposit_rate": bs.query_deposit_rate_data,
    "loan_rate": bs.query_loan_rate_data,
    "rrr": bs.query_required_reserve_ratio_data,
    "money_supply_month": bs.query_money_supply_data_month,
    "money_supply_year": bs.query_money_supply_data_year,
}


def query_macro(kind: str, start_date: str, end_date: str) -> pd.DataFrame:
    """宏观数据。kind 见 _MACRO_QUERIES；货币供应量日期格式 YYYY-MM / YYYY。"""
    return _query(
        _MACRO_QUERIES[kind], f"宏观 {kind} {start_date}~{end_date}",
        start_date=start_date, end_date=end_date,
    )
