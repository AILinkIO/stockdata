"""交易日工具（核心逻辑自 src/core/date_utils.py 移植，数据来自 PG 交易日历）。"""

from datetime import date, timedelta

from sqlalchemy import select

from api.services.readthrough import ensure_range, today
from db.models import TradeCalendar
from db.session import SyncSession

_LOOKBACK = 45  # 覆盖最长节假日间隔


def _trading_days(start: date, end: date) -> list[date]:
    ensure_range("trade_calendar", start, end)
    with SyncSession() as s:
        return list(
            s.scalars(
                select(TradeCalendar.calendar_date)
                .where(TradeCalendar.calendar_date >= start,
                       TradeCalendar.calendar_date <= end,
                       TradeCalendar.is_trading_day.is_(True))
                .order_by(TradeCalendar.calendar_date)
            ).all()
        )


def latest_trading_date() -> date:
    t = today()
    days = _trading_days(t - timedelta(days=_LOOKBACK), t)
    if not days:
        raise ValueError("交易日历数据缺失")
    return days[-1]


def is_trading_day(d: date) -> bool:
    return bool(_trading_days(d, d))


def previous_trading_day(d: date) -> date:
    days = _trading_days(d - timedelta(days=_LOOKBACK), d - timedelta(days=1))
    if not days:
        raise ValueError(f"{d} 之前 {_LOOKBACK} 天内无交易日")
    return days[-1]


def next_trading_day(d: date) -> date:
    days = _trading_days(d + timedelta(days=1), d + timedelta(days=_LOOKBACK))
    if not days:
        raise ValueError(f"{d} 之后 {_LOOKBACK} 天内无交易日")
    return days[0]


def last_n_trading_days(n: int) -> list[date]:
    if n <= 0:
        raise ValueError("days 必须为正数")
    t = today()
    days = _trading_days(t - timedelta(days=n * 3 + _LOOKBACK), t)
    return days[-n:]
