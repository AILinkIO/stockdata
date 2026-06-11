"""
定时同步任务（设计文档 4.4 节）。调度表见 fetcher/app.py 的 beat_schedule。

启动 beat（项目根目录）:
    uv run celery -A fetcher.app beat --loglevel=info
"""

import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import select

from db.models import TradeCalendar
from db.session import SyncSession
from fetcher.app import app

logger = logging.getLogger(__name__)

_CST = ZoneInfo("Asia/Shanghai")


def _today() -> date:
    return datetime.now(_CST).date()


def _is_trading_day(d: date) -> bool:
    with SyncSession() as s:
        return bool(
            s.execute(
                select(TradeCalendar.is_trading_day).where(
                    TradeCalendar.calendar_date == d
                )
            ).scalar()
        )


@app.task(name="fetcher.beat.sync_calendar")
def sync_calendar() -> dict:
    """每日 08:00：同步当年与次年交易日历（捕获临时调整）。"""
    t = _today()
    app.send_task(
        "fetcher.fetch_trade_calendar",
        kwargs={
            "start_date": date(t.year, 1, 1).isoformat(),
            "end_date": date(t.year + 1, 12, 31).isoformat(),
        },
    )
    return {"dispatched": "trade_calendar"}


@app.task(name="fetcher.beat.sync_market")
def sync_market() -> dict:
    """每交易日 17:00（收盘后）：股票列表、指数成分股、行业分类。"""
    t = _today()
    if not _is_trading_day(t):
        logger.info("今日 %s 非交易日，跳过市场数据同步", t)
        return {"skipped": "non-trading-day"}

    dispatched = []
    snap = t.isoformat()
    app.send_task("fetcher.fetch_stock_list", kwargs={"snap_date": snap})
    dispatched.append("stock_list")
    for index_code in ("sz50", "hs300", "zz500"):
        app.send_task(
            "fetcher.fetch_index_constituent",
            kwargs={"index_code": index_code, "snap_date": snap},
        )
        dispatched.append(f"index_{index_code}")
    app.send_task("fetcher.fetch_industry", kwargs={"snap_date": snap})
    dispatched.append("industry")
    return {"dispatched": dispatched}


@app.task(name="fetcher.beat.refresh_yesterday_list")
def refresh_yesterday_list() -> dict:
    """每日 08:30：补抓昨日股票列表（17:00 当日列表可能尚未发布，次日必有）。"""
    y = _today() - timedelta(days=1)
    if not _is_trading_day(y):
        return {"skipped": "non-trading-day"}
    app.send_task("fetcher.fetch_stock_list", kwargs={"snap_date": y.isoformat()})
    return {"dispatched": "stock_list", "snap_date": y.isoformat()}
