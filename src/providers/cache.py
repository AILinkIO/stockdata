"""
数据源缓存代理模块。

通过装饰器模式为 FinancialDataSource 添加 diskcache 缓存层，对上层完全透明。
详细缓存策略见 docs/cache-strategy.md。
"""
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from diskcache import Cache

from .interface import FinancialDataSource, NoDataFoundError

logger = logging.getLogger(__name__)

# ── TTL 常量（秒） ──
PERMANENT = 7776000          # 90 天 — 历史确定性数据兜底，防止无限膨胀
TTL_REALTIME = 300           # 5 分钟 — 盘中可能变化
TTL_DAILY = 86400            # 1 天
TTL_WEEKLY = 604800          # 7 天
TTL_ADJ_KLINE_MAX = 2592000  # 30 天 — 复权K线兜底清理（fingerprint 变化后旧条目的最大存活时间）

# get_fina_indicator 聚合查询的各类别方法与前缀（与 baostock._FINA_CATEGORIES 对应）
_FINA_CATEGORY_METHODS = [
    ("get_profit_data", "profit"),
    ("get_operation_data", "operation"),
    ("get_growth_data", "growth"),
    ("get_balance_data", "balance"),
    ("get_cash_flow_data", "cashflow"),
    ("get_dupont_data", "dupont"),
]

# 默认缓存配置
_DEFAULT_CACHE_DIR = Path(__file__).resolve().parent.parent.parent / ".cache" / "stockdata"
_DEFAULT_SIZE_LIMIT = 2**30  # 1 GiB


# ── 辅助函数 ──


def _today() -> datetime:
    return datetime.now()


def _parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """解析日期字符串，失败返回 None。"""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None


def _is_past_date(date_str: Optional[str]) -> bool:
    """判断日期是否在今天之前。None 视为"今天"（不可缓存）。"""
    d = _parse_date(date_str)
    return d is not None and d.date() < _today().date()


def _is_period_completed(end_date: str, frequency: str) -> bool:
    """判断 K 线周期是否已完成（最后一根K线已定型）。

    - 日K / 分钟K：end_date < 今天
    - 周K：end_date < 本周一
    - 月K：end_date < 本月1日
    """
    d = _parse_date(end_date)
    if d is None:
        return False
    today = _today().date()
    if frequency in ("d", "5", "15", "30", "60"):
        return d.date() < today
    if frequency == "w":
        this_monday = today - timedelta(days=today.weekday())
        return d.date() < this_monday
    if frequency == "m":
        first_of_month = today.replace(day=1)
        return d.date() < first_of_month
    return False


def _is_past_quarter(year: str, quarter: int) -> bool:
    """判断该季度的财报披露截止日是否已过。

    披露截止日：Q1→4/30, Q2→8/31, Q3→10/31, Q4→次年4/30
    """
    try:
        y = int(year)
    except (ValueError, TypeError):
        return False
    deadlines = {1: (y, 4, 30), 2: (y, 8, 31), 3: (y, 10, 31), 4: (y + 1, 4, 30)}
    deadline = deadlines.get(quarter)
    if not deadline:
        return False
    return _today().date() > datetime(*deadline).date()


def _is_macro_settled(end_date: Optional[str]) -> bool:
    """判断宏观数据日期是否在 2 个月前（发布滞后已消化）。"""
    d = _parse_date(end_date)
    if d is None:
        return False
    return d.date() < (_today() - timedelta(days=60)).date()


def _compute_adjust_fingerprint(adj_df: pd.DataFrame) -> tuple:
    """从复权因子 DataFrame 计算 fingerprint：(行数, 末行日期, 末行因子值)。

    fingerprint 变化意味着发生了新的除权事件，所有复权K线需要刷新。
    """
    if adj_df is None or adj_df.empty:
        return (0, "", "")
    last_row = adj_df.iloc[-1]
    date_col = "dividOperateDate" if "dividOperateDate" in adj_df.columns else adj_df.columns[0]
    factor_col = "foreAdjustFactor" if "foreAdjustFactor" in adj_df.columns else adj_df.columns[-1]
    return (len(adj_df), str(last_row.get(date_col, "")), str(last_row.get(factor_col, "")))


def _make_key(method_name: str, **kwargs) -> tuple:
    """构建缓存键：(方法名, 排序后的参数元组)。"""
    return (method_name, tuple(sorted(kwargs.items())))


# ── 缓存代理类 ──


class CachedDataSource(FinancialDataSource):
    """为 FinancialDataSource 添加 diskcache 缓存层的代理类。"""

    def __init__(self, delegate: FinancialDataSource, cache_dir: Path = _DEFAULT_CACHE_DIR,
                 size_limit: int = _DEFAULT_SIZE_LIMIT):
        self._delegate = delegate
        self._cache = Cache(str(cache_dir), size_limit=size_limit)
        expired = self._cache.expire()  # 启动时主动清理过期条目
        logger.info(f"数据缓存已启用，存储目录: {cache_dir}，大小限制: {size_limit / 2**20:.0f} MiB，"
                     f"本次清理过期条目: {expired} 条")

    def _get_or_fetch(self, method_name: str, ttl: Optional[int], **kwargs) -> pd.DataFrame:
        """缓存查找 → 未命中则调用底层数据源 → 写入缓存。"""
        key = _make_key(method_name, **kwargs)
        cached = self._cache.get(key)
        if cached is not None:
            logger.debug(f"缓存命中: {method_name}")
            return cached
        logger.debug(f"缓存未命中: {method_name}，正在查询数据源")
        df = getattr(self._delegate, method_name)(**kwargs)
        self._cache.set(key, df, expire=ttl)
        return df

    def _get_adjust_fingerprint(self, code: str, start_date: str, end_date: str) -> tuple:
        """获取复权因子 fingerprint，用于复权K线的缓存键。

        复权因子本身走缓存（历史日期永久，今天5分钟TTL）。
        """
        adj_df = self.get_adjust_factor_data(code=code, start_date=start_date, end_date=end_date)
        return _compute_adjust_fingerprint(adj_df)

    # ── 股票行情 ──

    def get_historical_k_data(self, code: str, start_date: str, end_date: str,
                              frequency: str = "d", adjust_flag: str = "3",
                              fields: Optional[list[str]] = None) -> pd.DataFrame:
        field_key = tuple(fields) if fields else None
        if adjust_flag == "3":
            # 不复权：按频率判断周期是否完成
            ttl = PERMANENT if _is_period_completed(end_date, frequency) else TTL_REALTIME
            return self._get_or_fetch(
                "get_historical_k_data", ttl,
                code=code, start_date=start_date, end_date=end_date,
                frequency=frequency, adjust_flag=adjust_flag, fields=field_key,
            )
        # 复权：将复权因子 fingerprint 嵌入缓存 key
        fp = self._get_adjust_fingerprint(code, start_date, end_date)
        key = _make_key(
            "get_historical_k_data",
            code=code, start_date=start_date, end_date=end_date,
            frequency=frequency, adjust_flag=adjust_flag, fields=field_key,
            _adjust_fp=fp,
        )
        cached = self._cache.get(key)
        if cached is not None:
            logger.debug(f"缓存命中: get_historical_k_data(复权) {code}")
            return cached
        logger.debug(f"缓存未命中: get_historical_k_data(复权) {code}，正在查询数据源")
        df = self._delegate.get_historical_k_data(
            code=code, start_date=start_date, end_date=end_date,
            frequency=frequency, adjust_flag=adjust_flag, fields=fields,
        )
        # 兜底 TTL 防止 fingerprint 变化后旧条目长期残留
        self._cache.set(key, df, expire=TTL_ADJ_KLINE_MAX)
        return df

    def get_stock_basic_info(self, code: str, fields: Optional[list[str]] = None) -> pd.DataFrame:
        return self._get_or_fetch(
            "get_stock_basic_info", TTL_DAILY,
            code=code, fields=tuple(fields) if fields else None,
        )

    def get_dividend_data(self, code: str, year: str, year_type: str = "report") -> pd.DataFrame:
        current_year = str(_today().year)
        ttl = PERMANENT if year < current_year else TTL_DAILY
        return self._get_or_fetch("get_dividend_data", ttl, code=code, year=year, year_type=year_type)

    def get_adjust_factor_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        ttl = PERMANENT if _is_past_date(end_date) else TTL_REALTIME
        return self._get_or_fetch("get_adjust_factor_data", ttl,
                                  code=code, start_date=start_date, end_date=end_date)

    # ── 财务报表 ──

    def _quarterly_ttl(self, year: str, quarter: int) -> Optional[int]:
        return PERMANENT if _is_past_quarter(year, quarter) else TTL_DAILY

    def get_profit_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        return self._get_or_fetch("get_profit_data", self._quarterly_ttl(year, quarter),
                                  code=code, year=year, quarter=quarter)

    def get_operation_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        return self._get_or_fetch("get_operation_data", self._quarterly_ttl(year, quarter),
                                  code=code, year=year, quarter=quarter)

    def get_growth_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        return self._get_or_fetch("get_growth_data", self._quarterly_ttl(year, quarter),
                                  code=code, year=year, quarter=quarter)

    def get_balance_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        return self._get_or_fetch("get_balance_data", self._quarterly_ttl(year, quarter),
                                  code=code, year=year, quarter=quarter)

    def get_cash_flow_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        return self._get_or_fetch("get_cash_flow_data", self._quarterly_ttl(year, quarter),
                                  code=code, year=year, quarter=quarter)

    def get_dupont_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        return self._get_or_fetch("get_dupont_data", self._quarterly_ttl(year, quarter),
                                  code=code, year=year, quarter=quarter)

    def get_performance_express_report(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        ttl = PERMANENT if _is_past_date(end_date) else TTL_DAILY
        return self._get_or_fetch("get_performance_express_report", ttl,
                                  code=code, start_date=start_date, end_date=end_date)

    def get_forecast_report(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        ttl = PERMANENT if _is_past_date(end_date) else TTL_DAILY
        return self._get_or_fetch("get_forecast_report", ttl,
                                  code=code, start_date=start_date, end_date=end_date)

    def get_fina_indicator(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """聚合综合财务指标，复用按季度缓存的各类财务数据。

        与 BaostockDataSource.get_fina_indicator 不同，此方法通过调用
        self.get_profit_data() 等已缓存的按季度方法组装结果，
        避免重复查询已缓存的季度数据。
        """
        current_year = str(_today().year)
        d = _parse_date(end_date)
        ttl = PERMANENT if (d and str(d.year) < current_year) else TTL_DAILY

        # 整体缓存检查
        key = _make_key("get_fina_indicator", code=code, start_date=start_date, end_date=end_date)
        cached = self._cache.get(key)
        if cached is not None:
            logger.debug("缓存命中: get_fina_indicator")
            return cached

        # 缓存未命中：从按季度缓存的子方法组装
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        records = []
        for year in range(start.year, end.year + 1):
            for quarter in range(1, 5):
                quarter_start = datetime(year, (quarter - 1) * 3 + 1, 1)
                if quarter_start > end:
                    continue
                logger.info(f"综合财务指标 {code} {year}Q{quarter}: 正在组装")
                record = self._build_fina_quarter(code, str(year), quarter)
                if record:
                    records.append(record)

        if not records:
            raise NoDataFoundError(f"综合财务指标 {code} {start_date}~{end_date}: 查询结果为空")

        df = pd.DataFrame(records)
        logger.info(f"综合财务指标 {code} {start_date}~{end_date}: 获取 {len(df)} 条记录")
        self._cache.set(key, df, expire=ttl)
        return df

    def _build_fina_quarter(self, code: str, year: str, quarter: int) -> dict | None:
        """从缓存层的按季度方法组装一条聚合财务指标记录。"""
        record: dict = {"code": code, "year": year, "quarter": quarter}
        for method_name, prefix in _FINA_CATEGORY_METHODS:
            try:
                df = getattr(self, method_name)(code=code, year=year, quarter=quarter)
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    for col in df.columns:
                        record[f"{prefix}_{col}"] = row[col]
            except Exception:
                pass  # 单类别失败不影响其他类别，与原逻辑一致
        if len(record) <= 3:
            return None
        return record

    # ── 市场概览 ──

    def get_trade_dates(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        ttl = PERMANENT if _is_past_date(end_date) else TTL_DAILY
        return self._get_or_fetch("get_trade_dates", ttl, start_date=start_date, end_date=end_date)

    def get_all_stock(self, date: Optional[str] = None) -> pd.DataFrame:
        ttl = PERMANENT if _is_past_date(date) else TTL_DAILY
        return self._get_or_fetch("get_all_stock", ttl, date=date)

    # ── 宏观经济 ──

    def _macro_ttl(self, end_date: Optional[str]) -> Optional[int]:
        return PERMANENT if _is_macro_settled(end_date) else TTL_WEEKLY

    def get_deposit_rate_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        return self._get_or_fetch("get_deposit_rate_data", self._macro_ttl(end_date),
                                  start_date=start_date, end_date=end_date)

    def get_loan_rate_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        return self._get_or_fetch("get_loan_rate_data", self._macro_ttl(end_date),
                                  start_date=start_date, end_date=end_date)

    def get_required_reserve_ratio_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None, year_type: str = '0') -> pd.DataFrame:
        return self._get_or_fetch("get_required_reserve_ratio_data", self._macro_ttl(end_date),
                                  start_date=start_date, end_date=end_date, year_type=year_type)

    def get_money_supply_data_month(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        return self._get_or_fetch("get_money_supply_data_month", self._macro_ttl(end_date),
                                  start_date=start_date, end_date=end_date)

    def get_money_supply_data_year(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        return self._get_or_fetch("get_money_supply_data_year", self._macro_ttl(end_date),
                                  start_date=start_date, end_date=end_date)

    # ── 指数与行业 ──

    def get_stock_industry(self, code: Optional[str] = None, date: Optional[str] = None) -> pd.DataFrame:
        ttl = PERMANENT if _is_past_date(date) else TTL_WEEKLY
        return self._get_or_fetch("get_stock_industry", ttl, code=code, date=date)

    def get_hs300_stocks(self, date: Optional[str] = None) -> pd.DataFrame:
        ttl = PERMANENT if _is_past_date(date) else TTL_DAILY
        return self._get_or_fetch("get_hs300_stocks", ttl, date=date)

    def get_sz50_stocks(self, date: Optional[str] = None) -> pd.DataFrame:
        ttl = PERMANENT if _is_past_date(date) else TTL_DAILY
        return self._get_or_fetch("get_sz50_stocks", ttl, date=date)

    def get_zz500_stocks(self, date: Optional[str] = None) -> pd.DataFrame:
        ttl = PERMANENT if _is_past_date(date) else TTL_DAILY
        return self._get_or_fetch("get_zz500_stocks", ttl, date=date)
