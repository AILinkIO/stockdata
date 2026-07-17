"""K 线页：/chart/{code}。频率 5/30/d/w，复权 不/前/后（读时计算）。"""

from __future__ import annotations

from datetime import date, timedelta

from nicegui import ui

from stockdata.db import queries
from stockdata.web import charts

from .common import nav

_FREQ_OPTIONS = {"5": "5分", "30": "30分", "d": "日K", "w": "周K"}
_ADJUST_OPTIONS = {"none": "不复权", "fore": "前复权", "back": "后复权"}
# 默认回看窗口：分钟线短、日/周线长
_DEFAULT_SPAN = {"5": 30, "30": 90, "d": 365, "w": 365 * 3}


@ui.page("/chart/{code}")
def chart_page(code: str) -> None:
    nav("chart")
    name = queries.security_name(code)
    today = date.today()

    with ui.column().classes("w-full max-w-6xl mx-auto p-4 gap-2"):
        with ui.row().classes("items-end gap-4 w-full"):
            ui.label(f"{code} {name}").classes("text-2xl font-bold")
            freq = ui.toggle(_FREQ_OPTIONS, value="d")
            adjust = ui.toggle(_ADJUST_OPTIONS, value="none")
            start_input = ui.input(
                "开始", value=(today - timedelta(days=_DEFAULT_SPAN["d"])).isoformat()
            ).classes("w-36")
            end_input = ui.input("结束", value=today.isoformat()).classes("w-36")

        hint = ui.label("").classes("text-sm text-gray-500")
        chart = ui.echart({}).classes("w-full").style("height: 640px")

        def render() -> None:
            try:
                start = date.fromisoformat(start_input.value)
                end = date.fromisoformat(end_input.value)
            except (TypeError, ValueError):
                hint.text = "日期格式：YYYY-MM-DD"
                return
            bars = queries.load_kline(code, freq.value, start, end)
            if bars.empty:
                hint.text = f"区间内无 {_FREQ_OPTIONS[freq.value]} 数据（还没同步？）"
                chart.options.clear()
                chart.update()
                return
            factors = queries.load_adjust_factors(code)
            adjusted = charts.apply_adjust(bars, factors, adjust.value)
            hint.text = f"{len(bars)} 根 · 复权因子 {len(factors)} 条 · {_ADJUST_OPTIONS[adjust.value]}"
            chart.options.clear()
            chart.options.update(charts.kline_option(code, name, freq.value, adjusted))
            chart.update()

        def on_freq_change() -> None:
            span = _DEFAULT_SPAN[freq.value]
            start_input.value = (today - timedelta(days=span)).isoformat()
            render()

        freq.on_value_change(on_freq_change)
        adjust.on_value_change(render)
        start_input.on("blur", render)
        end_input.on("blur", render)
        render()
