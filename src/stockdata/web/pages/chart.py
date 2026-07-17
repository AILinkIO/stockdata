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

            initial = {
                "from": (today - timedelta(days=_DEFAULT_SPAN["d"])).isoformat(),
                "to": today.isoformat(),
            }
            with ui.input(
                "区间", value=f"{initial['from']} ~ {initial['to']}"
            ).props("readonly").classes("w-64 cursor-pointer") as range_input:
                with ui.menu().props("no-parent-event") as range_menu:
                    date_picker = ui.date(value=initial).props("range")
                    with date_picker, ui.row().classes("w-full justify-end"):
                        ui.button("确定", on_click=range_menu.close).props(
                            "flat color=primary"
                        )
                with range_input.add_slot("append"):
                    ui.icon("edit_calendar").classes("cursor-pointer")
                range_input.on("click", range_menu.open)

        hint = ui.label("").classes("text-sm text-gray-500")
        chart = ui.echart({}).classes("w-full").style("height: 640px")

        with ui.expansion("完整历史数据", icon="table_view").classes("w-full"):
            data_table = ui.table(
                columns=[
                    {"name": "t", "label": "时间", "field": "t", "align": "left",
                     "sortable": True},
                    {"name": "open", "label": "开", "field": "open", "align": "right"},
                    {"name": "high", "label": "高", "field": "high", "align": "right"},
                    {"name": "low", "label": "低", "field": "low", "align": "right"},
                    {"name": "close", "label": "收", "field": "close", "align": "right"},
                    {"name": "volume", "label": "成交量", "field": "volume",
                     "align": "right"},
                    {"name": "amount", "label": "成交额", "field": "amount",
                     "align": "right"},
                ],
                rows=[],
                row_key="t",
                pagination={"rowsPerPage": 20, "sortBy": "t", "descending": True},
            ).classes("w-full")

        def _range() -> tuple[date, date] | None:
            v = date_picker.value
            if isinstance(v, str):  # 日历上只点了一天
                v = {"from": v, "to": v}
            if not isinstance(v, dict) or not v.get("from") or not v.get("to"):
                return None
            return date.fromisoformat(v["from"]), date.fromisoformat(v["to"])

        def render() -> None:
            span = _range()
            if span is None:
                hint.text = "请在日历中选择起止日期"
                return
            start, end = span
            range_input.value = f"{start.isoformat()} ~ {end.isoformat()}"
            bars = queries.load_kline(code, freq.value, start, end)
            if bars.empty:
                hint.text = f"区间内无 {_FREQ_OPTIONS[freq.value]} 数据（还没同步？）"
                chart.options.clear()
                chart.update()
                data_table.rows = []
                data_table.update()
                return
            factors = queries.load_adjust_factors(code)
            adjusted = charts.apply_adjust(bars, factors, adjust.value)
            hint.text = f"{len(bars)} 根 · 复权因子 {len(factors)} 条 · {_ADJUST_OPTIONS[adjust.value]}"
            chart.options.clear()
            chart.options.update(charts.kline_option(code, name, freq.value, adjusted))
            chart.update()
            data_table.rows = [
                {
                    "t": str(r["t"]),
                    "open": f"{r['open']:.2f}",
                    "high": f"{r['high']:.2f}",
                    "low": f"{r['low']:.2f}",
                    "close": f"{r['close']:.2f}",
                    "volume": int(r["volume"] or 0),
                    "amount": float(r["amount"] or 0),
                }
                for _, r in adjusted.iterrows()
            ]
            data_table.update()

        def on_freq_change() -> None:
            span = _DEFAULT_SPAN[freq.value]
            # 赋值触发 on_value_change → render，无需重复调用
            date_picker.value = {
                "from": (today - timedelta(days=span)).isoformat(),
                "to": today.isoformat(),
            }

        freq.on_value_change(on_freq_change)
        adjust.on_value_change(render)
        date_picker.on_value_change(render)
        render()
