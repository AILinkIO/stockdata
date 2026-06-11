"""
领域枚举。库中一律存 varchar（PG 原生 enum 加值需 DDL，不值得），
枚举约束只在 Python 层维护。
"""

from enum import StrEnum


class DataType(StrEnum):
    """data_watermark.data_type 的取值，覆盖所有数据集。"""

    # K 线（按频率）
    K_D = "k_d"
    K_W = "k_w"
    K_M = "k_m"
    K_5 = "k_5"
    K_15 = "k_15"
    K_30 = "k_30"
    K_60 = "k_60"

    ADJUST_FACTOR = "adjust_factor"
    DIVIDEND = "dividend"
    STOCK_BASIC = "stock_basic"

    # 财报（与 ReportType 一一对应）
    PROFIT = "profit"
    OPERATION = "operation"
    GROWTH = "growth"
    BALANCE = "balance"
    CASH_FLOW = "cash_flow"
    DUPONT = "dupont"
    EXPRESS = "express"
    FORECAST = "forecast"

    # 全市场数据集（watermark.code 为空串）
    TRADE_CALENDAR = "trade_calendar"
    STOCK_LIST = "stock_list"
    INDUSTRY = "industry"
    INDEX_SZ50 = "index_sz50"
    INDEX_HS300 = "index_hs300"
    INDEX_ZZ500 = "index_zz500"
    DEPOSIT_RATE = "deposit_rate"
    LOAN_RATE = "loan_rate"
    RRR = "rrr"
    MONEY_SUPPLY_MONTH = "money_supply_month"
    MONEY_SUPPLY_YEAR = "money_supply_year"

    @classmethod
    def from_k_frequency(cls, frequency: str) -> "DataType":
        return cls(f"k_{frequency}")


class ReportType(StrEnum):
    """financial_report.report_type 的取值。"""

    PROFIT = "profit"
    OPERATION = "operation"
    GROWTH = "growth"
    BALANCE = "balance"
    CASH_FLOW = "cash_flow"
    DUPONT = "dupont"
    EXPRESS = "express"
    FORECAST = "forecast"


class TaskStatus(StrEnum):
    """fetch_task.status 的取值。"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class IndexCode(StrEnum):
    """index_constituent.index_code 的取值。"""

    SZ50 = "sz50"
    HS300 = "hs300"
    ZZ500 = "zz500"
