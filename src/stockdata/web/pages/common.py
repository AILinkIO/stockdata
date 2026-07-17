"""页面公共件：导航栏 + 全局状态横幅（熔断/数据滞后，所有页面可见）。"""

from __future__ import annotations

import logging

from nicegui import ui

from stockdata.db import queries

logger = logging.getLogger(__name__)

LAG_WARN_TRADING_DAYS = 2  # 日K滞后超过该交易日数 → 黄条提醒


def nav(active: str) -> None:
    with ui.header().classes("items-center bg-primary text-white px-4"):
        ui.label("stockdata").classes("text-lg font-bold mr-6")
        for label, path, key in (
            ("关注列表", "/", "home"),
            ("同步", "/sync", "sync"),
        ):
            link = ui.link(label, path).classes("text-white mr-4 no-underline")
            if key == active:
                link.classes("font-bold underline")
    _status_banner()


def _status_banner() -> None:
    banner = ui.label("").classes("hidden")

    def refresh() -> None:
        try:
            h = queries.health_snapshot()
        except Exception:
            logger.exception("状态横幅查询失败")
            return
        base = "w-full px-4 py-2 font-bold rounded "
        if h["halt"]:
            banner.text = f"⛔ 同步熔断中：{h['halt'].get('reason', '?')}"
            banner.classes(replace=base + "bg-red-100 text-red-800")
        elif (h["max_lag_days"] or 0) > LAG_WARN_TRADING_DAYS:
            banner.text = (
                f"⚠️ 数据滞后：{h['lag_code']} 的日K已落后 {h['max_lag_days']} 个交易日，"
                f"到「同步」页跑一次关注列表同步"
            )
            banner.classes(replace=base + "bg-yellow-100 text-yellow-800")
        else:
            banner.classes(replace="hidden")

    refresh()
    ui.timer(30.0, refresh)
