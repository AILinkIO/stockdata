"""数据源异常层次与 Provider 协议。

- DataSourceError    — 数据源基础异常
  - LoginError       — 数据源登录失败
  - NoDataFoundError — 查询条件下无数据返回（同步引擎视为合法的 0 行结果）
  - BlacklistError   — 出口 IP 被 baostock 拉黑（10001011）或 10002007 熔断：
                       致命，不重试、不重登录，引擎写持久 halt 后停止。
"""

from __future__ import annotations

from typing import Protocol

import pandas as pd


class DataSourceError(Exception):
    """数据源基础异常类。"""


class LoginError(DataSourceError):
    """数据源登录失败时抛出。"""


class NoDataFoundError(DataSourceError):
    """查询条件下无数据返回时抛出。"""


class BlacklistError(DataSourceError):
    """出口 IP 被拉黑或持续接收错误：致命，引擎持久 halt 待人工 clear-halt。"""


class Provider(Protocol):
    """同步引擎依赖的查询接口（BaostockProvider / 测试 FakeProvider 共同实现）。

    所有日期参数为 'YYYY-MM-DD' 字符串；返回 DataFrame 的值全部为字符串
    （baostock 原始形态），schema/类型转换由 sync.writers 负责。
    """

    def query_k_data(
        self, code: str, start_date: str, end_date: str, frequency: str
    ) -> pd.DataFrame: ...

    def query_adjust_factor(self, code: str, start_date: str, end_date: str) -> pd.DataFrame: ...

    def query_stock_basic(self, code: str = "") -> pd.DataFrame: ...

    def query_dividend(self, code: str, year: str, year_type: str) -> pd.DataFrame: ...

    def query_fina_quarter(self, code: str, year: str, quarter: int) -> dict[str, dict]: ...

    def query_performance_express(
        self, code: str, start_date: str, end_date: str
    ) -> pd.DataFrame: ...

    def query_forecast(self, code: str, start_date: str, end_date: str) -> pd.DataFrame: ...

    def query_trade_dates(self, start_date: str, end_date: str) -> pd.DataFrame: ...

    def query_all_stock(self, date: str) -> pd.DataFrame: ...

    def query_industry(self, date: str) -> pd.DataFrame: ...

    def query_index_constituent(self, index_code: str, date: str) -> pd.DataFrame: ...

    def query_macro(self, kind: str, start_date: str, end_date: str) -> pd.DataFrame: ...

    def logout(self) -> None: ...
