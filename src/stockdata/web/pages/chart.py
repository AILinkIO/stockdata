"""单票页：/chart/{code}。Tab：K线 / 基本信息 / 财务指标 / 业绩 / 分红。

K 线频率 5/30/d/w，复权 不/前/后（读时计算）；其余 tab 展示同步入库的
基本信息（security+行业）、六类季度财报、业绩快报/预告与分红记录。
"""

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

_FIN_TYPES = {
    "profit": "盈利能力",
    "operation": "营运能力",
    "growth": "成长能力",
    "balance": "偿债能力",
    "cash_flow": "现金流量",
    "dupont": "杜邦指数",
}
_TYPE_LABELS = {1: "股票", 2: "指数", 3: "其他"}
_STATUS_LABELS = {1: "上市", 0: "退市"}

_EMPTY_HINT = "还没有数据（在「同步」页跑一次关注列表同步后这里会有内容）"


@ui.page("/chart/{code}")
def chart_page(code: str) -> None:
    nav("chart")
    name = queries.security_name(code)

    with ui.column().classes("w-full max-w-6xl mx-auto p-4 gap-2"):
        with ui.row().classes("items-center gap-4"):
            ui.label(f"{code} {name}").classes("text-2xl font-bold")
            options = {
                r["code"]: f"{r['code']} {r['code_name'] or ''}".strip()
                for r in queries.watchlist_overview()
            }
            options.setdefault(code, f"{code} {name}".strip())
            switcher = ui.select(
                options, value=code, with_input=True, label="切换股票"
            ).classes("w-56").props("dense outlined")
            switcher.on_value_change(
                lambda e: e.value and e.value != code
                and ui.navigate.to(f"/chart/{e.value}")
            )

        with ui.tabs().props("align=left") as tabs:
            tab_k = ui.tab("K线")
            tab_basic = ui.tab("基本信息")
            tab_fin = ui.tab("财务指标")
            tab_perf = ui.tab("业绩")
            tab_div = ui.tab("分红")

        with ui.tab_panels(tabs, value=tab_k).classes("w-full"):
            with ui.tab_panel(tab_k).classes("p-0"):
                _kline_panel(code, name)
            with ui.tab_panel(tab_basic):
                _basic_panel(code)
            with ui.tab_panel(tab_fin):
                _financial_panel(code)
            with ui.tab_panel(tab_perf):
                _performance_panel(code)
            with ui.tab_panel(tab_div):
                _dividend_panel(code)


# ── K线 ──


def _kline_panel(code: str, name: str) -> None:
    today = date.today()
    with ui.column().classes("w-full gap-2"):
        with ui.row().classes("items-end gap-4 w-full"):
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


# ── 基本信息 ──


def _basic_panel(code: str) -> None:
    info = queries.security_info(code)
    if info is None:
        ui.label(_EMPTY_HINT).classes("text-gray-400")
        return
    industry = info["industry"] or "—"
    if info["industry_snap_date"]:
        industry += f"（{info['industry_classification'] or '?'}，"
        industry += f"快照 {info['industry_snap_date']}）"
    fields = [
        ("代码", info["code"]),
        ("名称", info["code_name"] or "—"),
        ("类型", _TYPE_LABELS.get(info["type"], str(info["type"] or "—"))),
        ("状态", _STATUS_LABELS.get(info["status"], str(info["status"] or "—"))),
        ("上市日期", info["ipo_date"] or "—"),
        ("退市日期", info["out_date"] or "—"),
        ("所属行业", industry),
    ]
    with ui.grid(columns=2).classes("gap-x-12 gap-y-2 text-base"):
        for label, value in fields:
            ui.label(label).classes("text-gray-500")
            ui.label(str(value))


# ── 财务指标 / 业绩 / 分红（jsonb 动态列表格）──


def _jsonb_table(rows: list[dict], base_cols: list[tuple[str, str]],
                 json_field: str) -> None:
    """base_cols 固定列 + json 字段的键动态展开为列，横向可滚动。"""
    keys = sorted({k for r in rows for k in (r[json_field] or {})})
    columns = [
        {"name": n, "label": lbl, "field": n, "align": "left", "sortable": True}
        for n, lbl in base_cols
    ] + [{"name": k, "label": k, "field": k, "align": "right"} for k in keys]
    table_rows = [
        {
            **{n: r[n] or "—" for n, _ in base_cols},
            **{k: str(v) for k, v in (r[json_field] or {}).items()},
        }
        for r in rows
    ]
    with ui.element("div").classes("w-full overflow-x-auto"):
        ui.table(
            columns=columns, rows=table_rows, row_key=base_cols[0][0],
            pagination={"rowsPerPage": 12, "sortBy": base_cols[0][0],
                        "descending": True},
        ).props("dense").classes("w-full")


def _financial_panel(code: str) -> None:
    with ui.column().classes("w-full gap-2"):
        sel = ui.toggle(_FIN_TYPES, value="profit")
        body = ui.column().classes("w-full")

        def render() -> None:
            body.clear()
            with body:
                rows = queries.financial_reports(code, sel.value)
                if not rows:
                    ui.label(_EMPTY_HINT).classes("text-gray-400")
                    return
                _jsonb_table(
                    rows,
                    [("stat_date", "报告期"), ("pub_date", "发布日")],
                    "metrics",
                )

        sel.on_value_change(render)
        render()


def _performance_panel(code: str) -> None:
    with ui.column().classes("w-full gap-4"):
        for report_type, title in (
            ("performance_express", "业绩快报"),
            ("forecast", "业绩预告"),
        ):
            ui.label(title).classes("text-lg font-bold")
            rows = queries.financial_reports(code, report_type)
            if not rows:
                ui.label(_EMPTY_HINT).classes("text-gray-400")
                continue
            _jsonb_table(
                rows,
                [("stat_date", "报告期"), ("pub_date", "发布日")],
                "metrics",
            )


def _dividend_panel(code: str) -> None:
    rows = queries.dividends(code)
    if not rows:
        ui.label(_EMPTY_HINT).classes("text-gray-400")
        return
    _jsonb_table(
        rows,
        [
            ("plan_announce_date", "预案公告日"),
            ("year_type", "口径"),
            ("operate_date", "除权除息日"),
        ],
        "detail",
    )
