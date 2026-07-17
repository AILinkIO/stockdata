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

# jsonb 指标字段 → (中文表头, 格式)。格式决定数值换算，与表头单位保持一致：
#   pct   baostock 小数比率 → ×100 显示为 %
#   e8    元/股 → ÷1e8 显示为 亿
#   num   数值原样（保留原始精度、去尾零）
#   text  文本/日期原样
# 与固定列重复的 code/statDate/pubDate 等在 _EXCLUDED_KEYS 里剔除。
_EXCLUDED_KEYS = {
    "code", "statDate", "pubDate",
    "performanceExpPubDate", "performanceExpStatDate",
    "dividPlanAnnounceDate", "dividOperateDate",
}
_METRIC_DEFS: dict[str, tuple[str, str]] = {
    # 盈利能力
    "roeAvg": ("净资产收益率(%)", "pct"),
    "npMargin": ("销售净利率(%)", "pct"),
    "gpMargin": ("销售毛利率(%)", "pct"),
    "netProfit": ("净利润(亿元)", "e8"),
    "epsTTM": ("每股收益TTM(元)", "num"),
    "MBRevenue": ("主营营业收入(亿元)", "e8"),
    "totalShare": ("总股本(亿股)", "e8"),
    "liqaShare": ("流通股本(亿股)", "e8"),
    # 营运能力
    "NRTurnRatio": ("应收账款周转率(次)", "num"),
    "NRTurnDays": ("应收账款周转天数(天)", "num"),
    "INVTurnRatio": ("存货周转率(次)", "num"),
    "INVTurnDays": ("存货周转天数(天)", "num"),
    "CATurnRatio": ("流动资产周转率(次)", "num"),
    "AssetTurnRatio": ("总资产周转率(次)", "num"),
    # 成长能力
    "YOYEquity": ("净资产同比(%)", "pct"),
    "YOYAsset": ("总资产同比(%)", "pct"),
    "YOYNI": ("净利润同比(%)", "pct"),
    "YOYEPSBasic": ("基本每股收益同比(%)", "pct"),
    "YOYPNI": ("归母净利润同比(%)", "pct"),
    # 偿债能力
    "currentRatio": ("流动比率", "num"),
    "quickRatio": ("速动比率", "num"),
    "cashRatio": ("现金比率", "num"),
    "YOYLiability": ("总负债同比(%)", "pct"),
    "liabilityToAsset": ("资产负债率(%)", "pct"),
    "assetToEquity": ("权益乘数", "num"),
    # 现金流量
    "CAToAsset": ("流动资产/总资产(%)", "pct"),
    "NCAToAsset": ("非流动资产/总资产(%)", "pct"),
    "tangibleAssetToAsset": ("有形资产/总资产(%)", "pct"),
    "ebitToInterest": ("已获利息倍数(倍)", "num"),
    "CFOToOR": ("经营现金流/营业收入(%)", "pct"),
    "CFOToNP": ("经营现金流/净利润(%)", "pct"),
    "CFOToGr": ("经营现金流/营业总收入(%)", "pct"),
    # 杜邦指数
    "dupontROE": ("净资产收益率(%)", "pct"),
    "dupontAssetStoEquity": ("权益乘数", "num"),
    "dupontAssetTurn": ("总资产周转率(次)", "num"),
    "dupontPnitoni": ("归母净利/净利润(%)", "pct"),
    "dupontNitogr": ("净利润/营业总收入(%)", "pct"),
    "dupontTaxBurden": ("税收负担(%)", "pct"),
    "dupontIntburden": ("利息负担(%)", "pct"),
    "dupontEbittogr": ("EBIT/营业总收入(%)", "pct"),
    # 业绩快报
    "performanceExpUpdateDate": ("更新日期", "text"),
    "performanceExpressTotalAsset": ("总资产(亿元)", "e8"),
    "performanceExpressNetAsset": ("净资产(亿元)", "e8"),
    "performanceExpressEPSDiluted": ("每股收益·摊薄(元)", "num"),
    "performanceExpressROEWa": ("加权净资产收益率(%)", "num"),  # 源数据已是百分数
    "performanceExpressGRYOY": ("营业总收入同比(%)", "pct"),
    "performanceExpressOPYOY": ("营业利润同比(%)", "pct"),
    "performanceExpressEPSChgPct": ("每股收益同比(%)", "pct"),
    # 业绩预告
    "profitForcastType": ("预告类型", "text"),
    "profitForcastAbstract": ("预告摘要", "text"),
    "profitForcastChgPctUp": ("变动幅度上限(%)", "num"),
    "profitForcastChgPctDwn": ("变动幅度下限(%)", "num"),
    # 分红除权
    "dividPreNoticeDate": ("预披露公告日", "text"),
    "dividAgmPumDate": ("股东大会公告日", "text"),
    "dividPlanDate": ("分红实施公告日", "text"),
    "dividRegistDate": ("股权登记日", "text"),
    "dividPayDate": ("派息日", "text"),
    "dividStockMarketDate": ("红股上市日", "text"),
    "dividCashPsBeforeTax": ("每股股利·税前(元)", "num"),
    "dividCashPsAfterTax": ("每股股利·税后(元)", "num"),
    "dividStocksPs": ("每股红股(股)", "num"),
    "dividReserveToStockPs": ("每股转增(股)", "num"),
    "dividCashStock": ("分红送转说明", "text"),
}


def _fmt_metric(key: str, value) -> str:
    """按 _METRIC_DEFS 的单位换算数值；空值/非数值原样兜底。"""
    if value is None or value == "":
        return "—"
    kind = _METRIC_DEFS.get(key, ("", "text"))[1]
    if kind in ("pct", "e8"):
        try:
            v = float(value)
        except (TypeError, ValueError):
            return str(value)
        return f"{v * 100:.2f}" if kind == "pct" else f"{v / 1e8:.2f}"
    if kind == "num":
        try:
            return f"{float(value):.4f}".rstrip("0").rstrip(".")
        except (TypeError, ValueError):
            return str(value)
    return str(value)


@ui.page("/chart/{code}")
def chart_page(code: str) -> None:
    nav("chart")
    # 指标表：表头随列宽换行，整表自适应容器宽度
    ui.add_css(
        ".metrics-table th { white-space: normal; line-height: 1.3; }"
        ".metrics-table { width: 100%; }"
    )
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

        with ui.tab_panels(tabs, value=tab_k).classes("w-full mt-3"):
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
                    {"name": "open", "label": "开盘", "field": "open",
                     "align": "right"},
                    {"name": "high", "label": "最高", "field": "high",
                     "align": "right"},
                    {"name": "low", "label": "最低", "field": "low",
                     "align": "right"},
                    {"name": "close", "label": "收盘", "field": "close",
                     "align": "right"},
                    {"name": "volume", "label": "成交量(万股)", "field": "volume",
                     "align": "right"},
                    {"name": "amount", "label": "成交额(万元)", "field": "amount",
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
                    "volume": f"{float(r['volume'] or 0) / 1e4:,.2f}",
                    "amount": f"{float(r['amount'] or 0) / 1e4:,.2f}",
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
    """base_cols 固定列 + json 字段的键动态展开为列（中文表头+单位，数值随单位换算）。

    与固定列重复的键剔除；表头允许换行，列宽自适应容器不出横向滚动条。
    """
    keys = sorted(
        {k for r in rows for k in (r[json_field] or {})} - _EXCLUDED_KEYS,
        key=lambda k: (k not in _METRIC_DEFS, k),
    )
    columns = [
        {"name": n, "label": lbl, "field": n, "align": "left", "sortable": True}
        for n, lbl in base_cols
    ] + [
        {"name": k, "label": _METRIC_DEFS.get(k, (k,))[0], "field": k,
         "align": "right"}
        for k in keys
    ]
    table_rows = [
        {
            **{n: r[n] or "—" for n, _ in base_cols},
            **{k: _fmt_metric(k, (r[json_field] or {}).get(k)) for k in keys},
        }
        for r in rows
    ]
    ui.table(
        columns=columns, rows=table_rows, row_key=base_cols[0][0],
        pagination={"rowsPerPage": 12, "sortBy": base_cols[0][0],
                    "descending": True},
    ).props("dense wrap-cells").classes("w-full mt-2 metrics-table")


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
