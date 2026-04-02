"""
金融数据源抽象接口定义。

定义了 FinancialDataSource 抽象基类，规定所有数据源实现必须提供的方法。
当前实现为 BaostockDataSource（见 providers/baostock.py），
后续可扩展为 Akshare、Tushare 等其他数据源。

异常层次结构：
- DataSourceError    — 数据源基础异常
  - LoginError       — 数据源登录失败
  - NoDataFoundError — 查询条件下无数据返回
"""
from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd


class DataSourceError(Exception):
    """数据源基础异常类。"""


class LoginError(DataSourceError):
    """数据源登录失败时抛出。"""


class NoDataFoundError(DataSourceError):
    """查询条件下无数据返回时抛出。"""


class FinancialDataSource(ABC):
    """金融数据源抽象基类。

    定义了获取 A 股市场各类数据的标准接口，包括：
    - 股票行情（K线、基本信息、分红、复权因子）
    - 财务报表（利润、营运、成长、资产负债、现金流、杜邦分析）
    - 市场概览（交易日历、全部股票列表）
    - 宏观经济（存贷款利率、存款准备金率、货币供应量）
    - 指数与行业（成分股、行业分类）

    所有方法返回 pandas DataFrame，具体列名由实现类决定。
    """

    # ── 股票行情 ──

    @abstractmethod
    def get_historical_k_data(
        self,
        code: str,
        start_date: str,
        end_date: str,
        frequency: str = "d",
        adjust_flag: str = "3",
        fields: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """获取历史 K 线（OHLCV）数据。

        Args:
            code: 股票代码，如 'sh.600000'、'sz.000001'
            start_date: 起始日期，'YYYY-MM-DD' 格式
            end_date: 结束日期，'YYYY-MM-DD' 格式
            frequency: 数据频率（'d' 日 / 'w' 周 / 'm' 月 / '5'/'15'/'30'/'60' 分钟）
            adjust_flag: 复权类型（'1' 后复权 / '2' 前复权 / '3' 不复权）
            fields: 可选的字段列表，为 None 时使用实现类的默认字段
        """

    @abstractmethod
    def get_stock_basic_info(self, code: str) -> pd.DataFrame:
        """获取股票基本信息（名称、行业、上市日期等）。"""

    @abstractmethod
    def get_dividend_data(self, code: str, year: str, year_type: str = "report") -> pd.DataFrame:
        """获取分红送转数据。"""

    @abstractmethod
    def get_adjust_factor_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取复权因子数据。"""

    # ── 财务报表（按季度） ──

    @abstractmethod
    def get_profit_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        """获取季度盈利能力数据。"""

    @abstractmethod
    def get_operation_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        """获取季度营运能力数据。"""

    @abstractmethod
    def get_growth_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        """获取季度成长能力数据。"""

    @abstractmethod
    def get_balance_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        """获取季度偿债能力数据（资产负债表）。"""

    @abstractmethod
    def get_cash_flow_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        """获取季度现金流量数据。"""

    @abstractmethod
    def get_dupont_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        """获取季度杜邦分析数据。"""

    @abstractmethod
    def get_performance_express_report(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取业绩快报。"""

    @abstractmethod
    def get_forecast_report(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取业绩预告。"""

    @abstractmethod
    def get_fina_indicator(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取综合财务指标（聚合盈利、营运、成长、偿债、现金流、杜邦6大类）。"""

    # ── 市场概览 ──

    @abstractmethod
    def get_trade_dates(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取交易日历。"""

    @abstractmethod
    def get_all_stock(self, date: Optional[str] = None) -> pd.DataFrame:
        """获取全部股票列表及交易状态。"""

    # ── 宏观经济 ──

    @abstractmethod
    def get_deposit_rate_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取基准存款利率。"""

    @abstractmethod
    def get_loan_rate_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取基准贷款利率。"""

    @abstractmethod
    def get_required_reserve_ratio_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None, year_type: str = '0') -> pd.DataFrame:
        """获取存款准备金率。"""

    @abstractmethod
    def get_money_supply_data_month(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取月度货币供应量（M0/M1/M2）。"""

    @abstractmethod
    def get_money_supply_data_year(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """获取年度货币供应量（年末余额）。"""

    # ── 指数与行业 ──

    @abstractmethod
    def get_stock_industry(self, code: Optional[str] = None, date: Optional[str] = None) -> pd.DataFrame:
        """获取行业分类信息。"""

    @abstractmethod
    def get_hs300_stocks(self, date: Optional[str] = None) -> pd.DataFrame:
        """获取沪深300成分股。"""

    @abstractmethod
    def get_sz50_stocks(self, date: Optional[str] = None) -> pd.DataFrame:
        """获取上证50成分股。"""

    @abstractmethod
    def get_zz500_stocks(self, date: Optional[str] = None) -> pd.DataFrame:
        """获取中证500成分股。"""
