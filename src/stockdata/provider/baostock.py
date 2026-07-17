"""BaostockProvider：baostock 查询封装（自旧 server/fetcher/providers/baostock.py 类化移植）。

- 惰性登录（首次查询时 ensure_login），登录前经 SessionGuard 强制 ≥5 分钟间隔红线。
- 所有 baostock 操作（login / query / relogin）经模块级 _BS_LOCK 串行化——
  baostock 全局 TCP 连接非线程安全；配合"单 worker 线程"即双保险。
- 限流器在取锁前 acquire，避免持锁睡眠。
- 可重试错误（连接断开/未登录/超时）重登录后重试一次；仍失败抛 DataSourceError
  交由同步引擎退避重试。10001011 拉黑 / 10002007 连续熔断抛 BlacklistError。
- Watchdog 硬超时包住持锁段，兜底 baostock socketutil recv→b"" 死循环
  （socket 超时对空转 recv 无效）；超时视为连接报废，下次查询重新登录。
"""

from __future__ import annotations

import logging
import os
import socket
import sys
import threading
import time
from contextlib import contextmanager

import baostock as bs
import pandas as pd

from stockdata.config import Settings
from stockdata.core.ratelimit import MemoryRateLimiter
from stockdata.core.watchdog import Watchdog, WatchdogTimeout

from .interface import BlacklistError, DataSourceError, LoginError, NoDataFoundError
from .session_guard import SessionGuard

logger = logging.getLogger(__name__)

# 需要通过重连重试才能恢复的错误码
_RETRYABLE_CODES = frozenset(
    {
        "10001001",  # 用户未登录
        "10002001",  # 网络错误
        "10002002",  # 网络连接失败
        "10002004",  # 连接断开
        "10002007",  # 网络接收错误：长连接被服务端断开后 recv 空包，relogin 即恢复
    }
)

# 致命错误码：出口 IP 被真实拉黑。10002007 不在此列——实测可经 relogin 自愈，
# 仅在连续多次仍失败时由 _receive_error 升级为 BlacklistError（熔断）。
_BLACKLIST_CODES = frozenset({"10001011"})

# baostock 全局 TCP 连接非线程安全，模块级锁串行化所有实例的操作
_BS_LOCK = threading.RLock()


def _is_blacklist(code: str, msg: str) -> bool:
    return code in _BLACKLIST_CODES or "黑名单" in msg


def _is_retryable_error(exc: Exception) -> bool:
    if isinstance(exc, BlacklistError):  # 致命：绝不重试
        return False
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


# ── K 线默认字段（仅保留 5/30/d/w 四种频率）──

_DAILY_K_FIELDS = [
    "date", "code", "open", "high", "low", "close", "preclose", "volume",
    "amount", "adjustflag", "turn", "tradestatus", "pctChg",
    "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM", "isST",
]
_WEEKLY_K_FIELDS = [
    "date", "code", "open", "high", "low", "close", "volume", "amount",
    "adjustflag", "turn", "pctChg",
]
_MINUTE_K_FIELDS = [
    "date", "time", "code", "open", "high", "low", "close", "volume",
    "amount", "adjustflag",
]

SUPPORTED_FREQUENCIES = ("5", "30", "d", "w")


def _default_k_fields(frequency: str) -> list[str]:
    if frequency == "d":
        return _DAILY_K_FIELDS
    if frequency == "w":
        return _WEEKLY_K_FIELDS
    if frequency in ("5", "30"):
        return _MINUTE_K_FIELDS
    raise ValueError(f"不支持的 K 线频率: {frequency}（仅支持 {SUPPORTED_FREQUENCIES}）")


# 六类季度财报：(bs 查询函数名, report_type)。存函数名而非函数对象，便于测试替换 bs 模块。
FINA_CATEGORIES = [
    ("query_profit_data", "profit"),
    ("query_operation_data", "operation"),
    ("query_growth_data", "growth"),
    ("query_balance_data", "balance"),
    ("query_cash_flow_data", "cash_flow"),
    ("query_dupont_data", "dupont"),
]

INDEX_QUERIES = {
    "sz50": "query_sz50_stocks",
    "hs300": "query_hs300_stocks",
    "zz500": "query_zz500_stocks",
}

MACRO_QUERIES = {
    "deposit_rate": "query_deposit_rate_data",
    "loan_rate": "query_loan_rate_data",
    "rrr": "query_required_reserve_ratio_data",
    "money_supply_month": "query_money_supply_data_month",
    "money_supply_year": "query_money_supply_data_year",
}


class BaostockProvider:
    """唯一允许触碰 baostock 的对象。整个进程应只构造一个实例、只在 worker 线程使用。"""

    def __init__(
        self,
        settings: Settings,
        session_guard: SessionGuard,
        rate_limiter: MemoryRateLimiter | None = None,
    ) -> None:
        self._settings = settings
        self._guard = session_guard
        self.rate_limiter = rate_limiter or MemoryRateLimiter(settings.rate_limit_per_minute)
        self._logged_in = False
        self._last_activity = 0.0  # monotonic，空闲登出计时
        self._consecutive_recv_errors = 0

    # ── 登录 / 登出 ──

    def ensure_login(self) -> None:
        """确保已登录；登录前经 SessionGuard 补足 ≥5 分钟间隔。失败抛 LoginError。

        登录用 socket 默认超时包住：baostock 全局 TCP 连接在 login 时创建并
        继承该超时，之后所有查询的 recv 都受其约束。
        """
        if self._logged_in:
            return
        self._guard.before_login()
        prev = socket.getdefaulttimeout()
        socket.setdefaulttimeout(self._settings.baostock_socket_timeout)
        try:
            with _suppress_stdout():
                lg = bs.login()
        except OSError as e:  # 含 TimeoutError：连接/握手阶段网络故障
            raise LoginError(f"Baostock 登录失败: {e}") from e
        finally:
            socket.setdefaulttimeout(prev)
        if lg.error_code != "0":
            if _is_blacklist(lg.error_code, lg.error_msg):
                raise BlacklistError(
                    f"Baostock 登录被拉黑: {lg.error_msg} (code: {lg.error_code})"
                )
            raise LoginError(f"Baostock 登录失败: {lg.error_msg} (code: {lg.error_code})")
        self._logged_in = True
        self._stamp_activity()
        logger.info("Baostock 登录成功 (pid=%s)", os.getpid())

    def force_relogin(self) -> None:
        """放弃当前连接直接重新登录。

        故意不调用 bs.logout()：旧连接可能已死，logout 的 recv 会阻塞挂死；
        bs.login() 会新建 socket 替换模块级连接，旧会话由服务端回收。
        """
        self._logged_in = False
        self.ensure_login()

    def logout(self) -> None:
        """干净登出（进程关停/空闲到点时调用）。拿不到锁则只清标志。"""
        if _BS_LOCK.acquire(timeout=5):
            try:
                if self._logged_in:
                    with _suppress_stdout():
                        bs.logout()
                    logger.info("Baostock 已登出 (pid=%s)", os.getpid())
            except Exception:
                pass
            finally:
                self._logged_in = False
                _BS_LOCK.release()
        else:
            self._logged_in = False

    def should_idle_logout(self) -> bool:
        """空闲已超阈值且仍持登录态 → 应主动登出。纯时间戳比较、不触网。"""
        idle = self._settings.idle_logout_seconds
        if idle <= 0 or not self._logged_in:
            return False
        return time.monotonic() - self._last_activity >= idle

    def idle_logout(self) -> None:
        """空闲到点主动登出，避免复用服务端已断的僵死连接（10002007 常见来源）。

        bs.logout 在已断 socket 上同样可能 recv 空转，用 Watchdog 兜底。
        """
        logger.info("baostock 空闲 ≥ %ds，主动登出释放连接", self._settings.idle_logout_seconds)
        try:
            with Watchdog(threading.get_ident(), self._settings.watchdog_timeout_seconds):
                self.logout()
        except WatchdogTimeout:
            self._logged_in = False
            logger.warning("空闲登出被 watchdog 中断，已弃用连接")

    def reset_circuit(self) -> None:
        """熔断探测登录成功后调用：给恢复的会话重新一个完整的失败阈值窗口。

        普通 relogin 重试路径不重置——阈值升级语义靠它保证。
        """
        self._consecutive_recv_errors = 0

    # ── 内部状态 ──

    def _stamp_activity(self) -> None:
        self._last_activity = time.monotonic()

    def _receive_error(self, msg: str) -> Exception:
        """处理 10002007：未达阈值返回可重试 DataSourceError，达到则升级 BlacklistError。"""
        self._consecutive_recv_errors += 1
        threshold = self._settings.receive_error_halt_threshold
        if self._consecutive_recv_errors >= threshold:
            return BlacklistError(
                f"{msg}（连续 {self._consecutive_recv_errors} 次网络接收错误，"
                f"relogin 无法恢复，熔断暂停待自动探测或 clear-halt）",
                kind="login_error",
            )
        logger.warning(
            "网络接收错误(10002007) 第 %d 次，将重登录重试: %s",
            self._consecutive_recv_errors, msg,
        )
        return DataSourceError(msg)

    def _check_api_error(self, rs, description: str) -> None:
        if rs.error_code == "0":
            return
        msg = f"{description}: {rs.error_msg} (code: {rs.error_code})"
        if "no record found" in rs.error_msg.lower() or rs.error_code == "10002":
            raise NoDataFoundError(msg)
        if rs.error_code == "10002007":
            raise self._receive_error(msg)
        if _is_blacklist(rs.error_code, rs.error_msg):
            raise BlacklistError(msg)
        raise DataSourceError(msg)

    def _collect_rows(self, rs, description: str) -> pd.DataFrame:
        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            raise NoDataFoundError(f"{description}: 查询结果为空")
        df = pd.DataFrame(rows, columns=rs.fields)
        logger.info("%s: 获取 %d 条记录", description, len(df))
        return df

    # ── 通用查询流程 ──

    def _query(self, bs_func_name: str, description: str, **kwargs) -> pd.DataFrame:
        """限流 → 取锁 → 登录 → 查询；可重试错误重登录后重试一次；watchdog 硬超时兜底。"""
        self.rate_limiter.acquire()
        with _BS_LOCK:
            try:
                with Watchdog(
                    threading.get_ident(), self._settings.watchdog_timeout_seconds
                ):
                    return self._query_locked(bs_func_name, description, **kwargs)
            except WatchdogTimeout as e:
                # 连接已不可信：弃用，下次查询重新登录（受 SessionGuard 间隔约束）
                self._logged_in = False
                raise DataSourceError(f"{description}: watchdog 硬超时，连接已弃用") from e

    def _query_locked(self, bs_func_name: str, description: str, **kwargs) -> pd.DataFrame:
        self.ensure_login()
        logger.debug("正在查询 %s", description)
        bs_func = getattr(bs, bs_func_name)

        def _do() -> pd.DataFrame:
            rs = bs_func(**kwargs)
            self._check_api_error(rs, description)
            return self._collect_rows(rs, description)

        try:
            df = _do()
        except NoDataFoundError:
            raise
        except Exception as e:
            if not _is_retryable_error(e):
                if isinstance(e, DataSourceError):
                    raise
                raise DataSourceError(f"{description}: 未预期错误 - {e}") from e
            logger.warning("%s: 可重试错误，重新登录后重试一次: %s", description, e)
            self.force_relogin()
            df = _do()
        self._stamp_activity()               # 成功取数：重置空闲计时
        self._consecutive_recv_errors = 0    # 成功取数：清零熔断计数（登录成功不清零）
        return df

    # ── 查询接口（Provider 协议实现）──

    def query_k_data(
        self, code: str, start_date: str, end_date: str, frequency: str
    ) -> pd.DataFrame:
        """K 线，恒为不复权（adjustflag=3，复权读时计算）。"""
        return self._query(
            "query_history_k_data_plus",
            f"K线 {code} {frequency} {start_date}~{end_date}",
            code=code,
            fields=",".join(_default_k_fields(frequency)),
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            adjustflag="3",
        )

    def query_adjust_factor(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._query(
            "query_adjust_factor",
            f"复权因子 {code} {start_date}~{end_date}",
            code=code, start_date=start_date, end_date=end_date,
        )

    def query_stock_basic(self, code: str = "") -> pd.DataFrame:
        if code:
            return self._query("query_stock_basic", f"基本信息 {code}", code=code)
        return self._query("query_stock_basic", "基本信息（全市场）")

    def query_dividend(self, code: str, year: str, year_type: str) -> pd.DataFrame:
        return self._query(
            "query_dividend_data",
            f"分红 {code} {year}",
            code=code, year=year, yearType=year_type,
        )

    def query_fina_quarter(self, code: str, year: str, quarter: int) -> dict[str, dict]:
        """单季度六类财务数据。返回 {report_type: {field: value}}，无数据的类别缺席。"""
        result: dict[str, dict] = {}
        for bs_func_name, report_type in FINA_CATEGORIES:
            desc = f"{report_type} {code} {year}Q{quarter}"
            try:
                df = self._query(bs_func_name, desc, code=code, year=year, quarter=quarter)
                result[report_type] = df.iloc[0].to_dict()
            except NoDataFoundError:
                logger.debug("%s: 无数据", desc)
        return result

    def query_performance_express(
        self, code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        return self._query(
            "query_performance_express_report",
            f"业绩快报 {code} {start_date}~{end_date}",
            code=code, start_date=start_date, end_date=end_date,
        )

    def query_forecast(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._query(
            "query_forecast_report",
            f"业绩预告 {code} {start_date}~{end_date}",
            code=code, start_date=start_date, end_date=end_date,
        )

    def query_trade_dates(self, start_date: str, end_date: str) -> pd.DataFrame:
        return self._query(
            "query_trade_dates",
            f"交易日历 {start_date}~{end_date}",
            start_date=start_date, end_date=end_date,
        )

    def query_all_stock(self, date: str) -> pd.DataFrame:
        return self._query("query_all_stock", f"全部股票 {date}", day=date)

    def query_industry(self, date: str) -> pd.DataFrame:
        return self._query("query_stock_industry", f"行业分类 {date}", date=date)

    def query_index_constituent(self, index_code: str, date: str) -> pd.DataFrame:
        return self._query(
            INDEX_QUERIES[index_code], f"{index_code}成分股 {date}", date=date
        )

    def query_macro(self, kind: str, start_date: str, end_date: str) -> pd.DataFrame:
        """宏观数据。kind 见 MACRO_QUERIES；货币供应量日期格式 YYYY-MM / YYYY。"""
        return self._query(
            MACRO_QUERIES[kind], f"宏观 {kind} {start_date}~{end_date}",
            start_date=start_date, end_date=end_date,
        )
