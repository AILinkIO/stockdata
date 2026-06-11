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


@app.task(name="fetcher.beat.sync_tracked_codes")
def sync_tracked_codes() -> dict:
    """每交易日 17:10：增量同步已入库代码的交易信息。

    "已入库代码" = data_watermark 中存在 K线/复权因子水位的 code（即曾被查询过、
    系统持续跟踪的标的）。对每个 (code, 数据类型) 投递增量抓取：
    水位 last_date（含，覆写盘中写入的当日未收盘 bar）→ 今天。
    任务经分片路由，同 code 同类型落在同一 worker 进程串行执行。
    """
    t = _today()
    if not _is_trading_day(t):
        logger.info("今日 %s 非交易日，跳过已入库代码同步", t)
        return {"skipped": "non-trading-day"}

    from db.models import DataWatermark

    k_types = {"k_d": "d", "k_w": "w", "k_m": "m"}
    minute_types = {"k_5": 5, "k_15": 15, "k_30": 30, "k_60": 60}
    dispatched = 0
    with SyncSession() as s:
        rows = s.execute(
            select(DataWatermark.code, DataWatermark.data_type, DataWatermark.last_date)
            .where(
                DataWatermark.code != "",
                DataWatermark.data_type.in_([*k_types, *minute_types, "adjust_factor"]),
                # 不过滤 last_date == 今天：盘中抓过的当日 bar 正需要收盘后覆写
            )
        ).all()

    for code, data_type, last_date in rows:
        start = last_date.isoformat()  # 含 last_date：覆写可能的盘中数据
        end = t.isoformat()
        if data_type in k_types:
            app.send_task("fetcher.fetch_kline", kwargs={
                "code": code, "start_date": start, "end_date": end,
                "frequency": k_types[data_type],
            })
        elif data_type in minute_types:
            app.send_task("fetcher.fetch_kline_minute", kwargs={
                "code": code, "start_date": start, "end_date": end,
                "frequency": minute_types[data_type],
            })
        else:  # adjust_factor：除权事件检测
            app.send_task("fetcher.fetch_adjust_factor", kwargs={
                "code": code, "start_date": start, "end_date": end,
            })
        dispatched += 1

    logger.info("已入库代码同步：投递 %d 个增量任务", dispatched)
    return {"dispatched": dispatched}


@app.task(name="fetcher.beat.refresh_yesterday_list")
def refresh_yesterday_list() -> dict:
    """每日 08:30：补抓昨日股票列表（17:00 当日列表可能尚未发布，次日必有）。"""
    y = _today() - timedelta(days=1)
    if not _is_trading_day(y):
        return {"skipped": "non-trading-day"}
    app.send_task("fetcher.fetch_stock_list", kwargs={"snap_date": y.isoformat()})
    return {"dispatched": "stock_list", "snap_date": y.isoformat()}
