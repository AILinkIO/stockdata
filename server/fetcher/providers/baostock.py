"""
Baostock 查询函数。

模块级单例连接，惰性登录（首次查询时 ensure_login）。所有 baostock 操作
（login / query / relogin）通过 _BS_LOCK 串行化——baostock 全局 TCP 连接
非线程安全，FastAPI 线程池的并发请求必须排队。

挂死由 socket 超时（setdefaulttimeout）快速失败 + 重连重试兜底。
可重试错误（连接断开/未登录）重登录后重试一次；仍失败则抛出 DataSourceError，
交由任务层（_run）退避重试或最终失败。
"""

import logging
import os
import socket
import sys
import threading
from contextlib import contextmanager

import baostock as bs
import pandas as pd

from core.ratelimit import create_rate_limiter
from settings import settings

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

_logged_in = False
_BS_LOCK = threading.RLock()

_RATE_LIMITER = create_rate_limiter(
    max_calls=settings.fetch_rate_limit_per_minute,
    backend=settings.rate_limit_backend,
    redis_url=settings.rate_limit_redis_url,
    key="ratelimit:baostock",
)


def _is_retryable_error(exc: Exception) -> bool:
    if isinstance(exc, TimeoutError):  # socket 超时（socket.timeout 是其别名）
        return True
    msg = str(exc)
    return (
        any(code in msg for code in _RETRYABLE_CODES)
        or "login" in msg.lower()
        or "未登录" in msg
        or "Broken pipe" in msg
    )


@contextmanager
def _suppress_stdout():
    """屏蔽 baostock login/logout 直接 print 到 stdout 的噪音。"""
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
    """确保已登录。失败抛 LoginError（任务层会重试/标记失败）。

    登录用 socket 默认超时包住：baostock 的全局 TCP 连接在 login 时创建，
    创建时继承该超时，之后所有查询的 recv 都受其约束——挂死在超时处快速
    失败走重连重试。
    """
    global _logged_in
    if _logged_in:
        return
    prev = socket.getdefaulttimeout()
    socket.setdefaulttimeout(settings.baostock_socket_timeout)
    try:
        with _suppress_stdout():
            lg = bs.login()
    except OSError as e:  # 含 TimeoutError：连接/握手阶段网络故障
        raise LoginError(f"Baostock 登录失败: {e}") from e
    finally:
        socket.setdefaulttimeout(prev)
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

    长连接保持：不做闲置预防性重登录，连接一直复用到出错为止；可重试错误
    （连接断开/未登录/超时）就地重登录并重试一次（处理闲置后连接僵死的常见情况，
    无需等待）。仍失败则抛 DataSourceError，由任务层 _run 退避重试。
    _BS_LOCK 串行化所有 baostock 操作（非线程安全）。
    限流（_RATE_LIMITER）在取锁前阻塞，避免持锁睡眠。
    """
    _RATE_LIMITER.acquire()
    with _BS_LOCK:
        ensure_login()
        logger.info("正在查询 %s", description)

        def _do() -> pd.DataFrame:
            rs = bs_func(**kwargs)
            _check_api_error(rs, description)
            df = _collect_rows(rs, description)
            return df

        try:
            return _do()
        except NoDataFoundError:
            raise
        except Exception as e:
            if not _is_retryable_error(e):
                if isinstance(e, DataSourceError):
                    raise
                raise DataSourceError(f"{description}: 未预期错误 - {e}") from e
            logger.warning("%s: 可重试错误，重新登录后重试一次: %s", description, e)
            force_relogin()
            return _do()


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
