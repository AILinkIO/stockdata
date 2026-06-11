"""K 线读取与复权计算。

复权公式（已对 baostock 输出实证）：bar 的复权价 = 不复权价 × 因子，
因子取除权日 ≤ bar 日期的最近一次事件（前复权用 fore、后复权用 back），
首个事件之前因子为 1。
"""

from bisect import bisect_right
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import select

from api.services.readthrough import ensure_range
from db.models import AdjustFactor, Kline, KlineMinute
from db.session import SyncSession

_PRICE_COLS = ("open", "high", "low", "close", "preclose")


def _row_dict(obj, cols) -> dict:
    return {c: getattr(obj, c) for c in cols}

_K_COLS = [c.name for c in Kline.__table__.columns if c.name != "updated_at"]
_KM_COLS = [c.name for c in KlineMinute.__table__.columns if c.name != "updated_at"]


def get_adjust_factors(code: str, start: date, end: date) -> list[dict]:
    ensure_range("adjust_factor", start, end, code)
    with SyncSession() as s:
        rows = s.scalars(
            select(AdjustFactor)
            .where(AdjustFactor.code == code,
                   AdjustFactor.divid_operate_date >= start,
                   AdjustFactor.divid_operate_date <= end)
            .order_by(AdjustFactor.divid_operate_date)
        ).all()
    return [
        _row_dict(r, ["code", "divid_operate_date", "fore_adjust_factor",
                      "back_adjust_factor", "adjust_factor"])
        for r in rows
    ]


def _load_factors_for_adjust(code: str, end: date) -> tuple[list[date], list[tuple[Decimal, Decimal]]]:
    """复权需要 [上市以来, end] 的完整因子序列（lookup 用），返回 (事件日列表, (fore, back) 列表)。"""
    ensure_range("adjust_factor", date(1990, 12, 19), end, code)
    with SyncSession() as s:
        rows = s.execute(
            select(AdjustFactor.divid_operate_date,
                   AdjustFactor.fore_adjust_factor,
                   AdjustFactor.back_adjust_factor)
            .where(AdjustFactor.code == code, AdjustFactor.divid_operate_date <= end)
            .order_by(AdjustFactor.divid_operate_date)
        ).all()
    return [r[0] for r in rows], [(r[1], r[2]) for r in rows]


def _factor_at(event_dates: list[date], factors: list, d: date, flag: str) -> Decimal:
    i = bisect_right(event_dates, d)
    if i == 0:
        return Decimal(1)
    fore, back = factors[i - 1]
    return fore if flag == "2" else back


def get_kline(code: str, start: date, end: date,
              frequency: str = "d", adjust_flag: str = "3") -> list[dict]:
    """日/周/月 K 线。adjust_flag: 1 后复权 / 2 前复权 / 3 不复权。"""
    ensure_range(f"k_{frequency}", start, end, code)
    with SyncSession() as s:
        rows = s.scalars(
            select(Kline)
            .where(Kline.code == code, Kline.frequency == frequency,
                   Kline.trade_date >= start, Kline.trade_date <= end)
            .order_by(Kline.trade_date)
        ).all()
    out = [_row_dict(r, _K_COLS) for r in rows]

    if adjust_flag in ("1", "2") and out:
        event_dates, factors = _load_factors_for_adjust(code, end)
        for row in out:
            f = _factor_at(event_dates, factors, row["trade_date"], adjust_flag)
            for col in _PRICE_COLS:
                if row.get(col) is not None:
                    row[col] = row[col] * f
    return out


def get_kline_minute(code: str, start: date, end: date, frequency: int = 30) -> list[dict]:
    """分钟 K 线（仅不复权：分钟级复权无业务意义且旧实现亦未支持精确口径）。"""
    ensure_range(f"k_{frequency}", start, end, code)
    with SyncSession() as s:
        rows = s.scalars(
            select(KlineMinute)
            .where(KlineMinute.code == code, KlineMinute.frequency == frequency,
                   # date 与 timestamptz 比较：end 当天 00:00 之后的 bar 也属于 end 日，右开区间
                   KlineMinute.bar_time >= start,
                   KlineMinute.bar_time < end + timedelta(days=1))
            .order_by(KlineMinute.bar_time)
        ).all()
    return [_row_dict(r, _KM_COLS) for r in rows]
