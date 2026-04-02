"""
金融数据源抽象接口定义。

定义了 FinancialDataSource 抽象基类，规定所有数据源实现必须提供的方法。
当前实现为 BaostockDataSource（见 providers/baostock.py），
后续可扩展为 Akshare、Tushare 等其他数据源。

同时定义了数据源层的异常层次结构：
- DataSourceError    — 数据源基础异常
  - LoginError       — 数据源登录失败
  - NoDataFoundError — 查询条件下无数据返回
"""
from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, List


class DataSourceError(Exception):
    """数据源基础异常类。"""
    pass


class LoginError(DataSourceError):
    """数据源登录失败时抛出。"""
    pass


class NoDataFoundError(DataSourceError):
    """查询条件下无数据返回时抛出。"""
    pass


class FinancialDataSource(ABC):
    """
    金融数据源抽象基类。

    定义了获取 A 股市场各类数据的标准接口，包括：
    - 股票行情（K线、基本信息、分红、复权因子）
    - 财务报表（利润、营运、成长、资产负债、现金流、杜邦分析）
    - 市场概览（交易日历、全部股票列表）
    - 宏观经济（存贷款利率、存款准备金率、货币供应量）
    - 指数与行业（成分股、行业分类）

    所有方法返回 pandas DataFrame，具体列名由实现类决定。
    """

    @abstractmethod
    def get_historical_k_data(
        self,
        code: str,
        start_date: str,
        end_date: str,
        frequency: str = "d",
        adjust_flag: str = "3",
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Fetches historical K-line (OHLCV) data for a given stock code.

        Args:
            code: The stock code (e.g., 'sh.600000', 'sz.000001').
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.
            frequency: Data frequency. Common values depend on the underlying
                       source (e.g., 'd' for daily, 'w' for weekly, 'm' for monthly,
                       '5', '15', '30', '60' for minutes). Defaults to 'd'.
            adjust_flag: Adjustment flag for historical data. Common values
                         depend on the source (e.g., '1' for forward adjusted,
                         '2' for backward adjusted, '3' for non-adjusted).
                         Defaults to '3'.
            fields: Optional list of specific fields to retrieve. If None,
                    retrieves default fields defined by the implementation.

        Returns:
            A pandas DataFrame containing the historical K-line data, with
            columns corresponding to the requested fields.

        Raises:
            LoginError: If login to the data source fails.
            NoDataFoundError: If no data is found for the query.
            DataSourceError: For other data source related errors.
            ValueError: If input parameters are invalid.
        """
        pass

    @abstractmethod
    def get_stock_basic_info(self, code: str) -> pd.DataFrame:
        """
        Fetches basic information for a given stock code.

        Args:
            code: The stock code (e.g., 'sh.600000', 'sz.000001').

        Returns:
            A pandas DataFrame containing the basic stock information.
            The structure and columns depend on the underlying data source.
            Typically contains info like name, industry, listing date, etc.

        Raises:
            LoginError: If login to the data source fails.
            NoDataFoundError: If no data is found for the query.
            DataSourceError: For other data source related errors.
            ValueError: If the input code is invalid.
        """
        pass

    @abstractmethod
    def get_trade_dates(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches trading dates information within a range."""
        pass

    @abstractmethod
    def get_all_stock(self, date: Optional[str] = None) -> pd.DataFrame:
        """Fetches list of all stocks and their trading status on a given date."""
        pass

    @abstractmethod
    def get_deposit_rate_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches benchmark deposit rates."""
        pass

    @abstractmethod
    def get_loan_rate_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches benchmark loan rates."""
        pass

    @abstractmethod
    def get_required_reserve_ratio_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None, year_type: str = '0') -> pd.DataFrame:
        """Fetches required reserve ratio data."""
        pass

    @abstractmethod
    def get_money_supply_data_month(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches monthly money supply data (M0, M1, M2)."""
        pass

    @abstractmethod
    def get_money_supply_data_year(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches yearly money supply data (M0, M1, M2 - year end balance)."""
        pass

    @abstractmethod
    def get_dividend_data(self, code: str, year: str, year_type: str = "report") -> pd.DataFrame:
        """Fetches dividend information for a stock and year."""
        pass

    @abstractmethod
    def get_adjust_factor_data(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetches adjustment factor data used for price adjustments."""
        pass

    # Financial report datasets
    @abstractmethod
    def get_profit_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_operation_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_growth_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_balance_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_cash_flow_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_dupont_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_performance_express_report(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_forecast_report(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_fina_indicator(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetches financial indicators (ROE, gross margin, net margin, etc.) within a date range.

        Args:
            code: The stock code (e.g., 'sh.600000', 'sz.000001').
            start_date: Start date in 'YYYY-MM-DD' format.
            end_date: End date in 'YYYY-MM-DD' format.

        Returns:
            A pandas DataFrame containing financial indicators such as:
            - roe, roe_yearly (Return on Equity)
            - netprofit_margin, grossprofit_margin (Profitability ratios)
            - expense_ratio, netprofit_ratio
            - current_ratio, quick_ratio (Liquidity ratios)
            - etc.
        """
        pass

    # Index / industry
    @abstractmethod
    def get_stock_industry(self, code: Optional[str] = None, date: Optional[str] = None) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_hs300_stocks(self, date: Optional[str] = None) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_sz50_stocks(self, date: Optional[str] = None) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_zz500_stocks(self, date: Optional[str] = None) -> pd.DataFrame:
        pass

    # Market overview
    @abstractmethod
    def get_all_stock(self, date: Optional[str] = None) -> pd.DataFrame:
        pass
    # Note: SHIBOR is not implemented in current Baostock bindings; no abstract method here.
