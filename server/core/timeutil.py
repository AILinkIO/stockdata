"""统一业务时间定义：全项目的"现在/今天"一律取中国时区（Asia/Shanghai）。

A 股业务日期（交易日、定型边界、水位）都以中国时区为准；此前各模块
（readthrough/tasks/beat/writer）重复定义 _CST 与 today，统一收口于此。
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

CST = ZoneInfo("Asia/Shanghai")


def now_cst() -> datetime:
    return datetime.now(CST)


def today_cst() -> date:
    return now_cst().date()
