"""ORM 模型聚合导出。import 本包即注册全部表到 Base.metadata（Alembic 依赖此行为）。"""

from .base import Base
from .enums import DataType, IndexCode, ReportType, TaskStatus
from .kline import Kline, KlineMinute
from .adjust import AdjustFactor, Dividend
from .financial import FinancialReport
from .market import (
    IndexConstituent,
    StockBasic,
    StockIndustry,
    StockListSnapshot,
    TradeCalendar,
)
from .macro import (
    DepositRate,
    LoanRate,
    MoneySupplyMonth,
    MoneySupplyYear,
    RequiredReserveRatio,
)
from .meta import DataWatermark, FetchTask

__all__ = [
    "Base",
    "DataType",
    "IndexCode",
    "ReportType",
    "TaskStatus",
    "Kline",
    "KlineMinute",
    "AdjustFactor",
    "Dividend",
    "FinancialReport",
    "StockBasic",
    "TradeCalendar",
    "StockListSnapshot",
    "IndexConstituent",
    "StockIndustry",
    "DepositRate",
    "LoanRate",
    "RequiredReserveRatio",
    "MoneySupplyMonth",
    "MoneySupplyYear",
    "DataWatermark",
    "FetchTask",
]
