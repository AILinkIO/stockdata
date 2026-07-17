"""同步仪表盘页：/sync。与 CLI TUI 展示同一份 RunState（ui.timer 轮询）。

三块内容：当前进度、市场级任务（默认全市场指标，单独列出）、运行历史（点击看详情）。
"""

from __future__ import annotations

import time

from nicegui import ui

from stockdata.config import settings
from stockdata.db import queries
from stockdata.sync.datasets import MARKET_HANDLERS
from stockdata.sync.engine import RunParams, clear_halt, read_halt

from .. import state
from .common import nav

# 数据集中文名（UI 展示用）
DATASET_LABELS = {
    "trade_calendar": "交易日历",
    "security": "证券基本信息",
    "stock_list": "全市场股票列表",
    "industry": "行业分类",
    "index_sz50": "上证50成分股",
    "index_hs300": "沪深300成分股",
    "index_zz500": "中证500成分股",
    "macro_deposit_rate": "存款基准利率",
    "macro_loan_rate": "贷款基准利率",
    "macro_rrr": "存款准备金率",
    "macro_money_supply_month": "货币供应量（月）",
    "macro_money_supply_year": "货币供应量（年）",
    "stock_basic": "个股基本资料",
    "k_d": "日K线",
    "k_w": "周K线",
    "k_5": "5分钟线",
    "k_30": "30分钟线",
    "adjust_factor": "复权因子",
    "dividend": "分红除权",
    "financial": "季度财务",
    "performance_express": "业绩快报",
    "forecast": "业绩预告",
}

_MARKET_DATASETS = [h.dataset.value for h in MARKET_HANDLERS]


def _label(dataset: str) -> str:
    return DATASET_LABELS.get(dataset, dataset)


@ui.page("/sync")
def sync_page() -> None:
    nav("sync")
    with ui.column().classes("w-full max-w-5xl mx-auto p-4 gap-4"):
        ui.label("同步").classes("text-2xl font-bold")

        halt_banner = ui.label("").classes(
            "w-full p-2 rounded bg-red-100 text-red-800 font-bold hidden"
        )

        # ── 控制区 ──
        with ui.row().classes("items-center gap-2"):

            def start_watchlist() -> None:
                _start(RunParams(watchlist_only=True), "关注列表同步")

            def start_market() -> None:
                _start(RunParams(datasets=list(_MARKET_DATASETS)), "全市场指标同步")

            def _start(params: RunParams, what: str) -> None:
                if _halt():
                    ui.notify("处于熔断状态，先清除熔断", type="warning")
                    return
                ok, msg = state.get_runner().start(params)
                ui.notify(f"已启动{what}" if ok else f"未启动：{msg}",
                          type="positive" if ok else "warning")

            def stop_run() -> None:
                if state.get_runner().stop():
                    ui.notify("已请求停止（完成当前切片后退出）")
                else:
                    ui.notify("当前没有在运行的任务", type="info")

            def do_clear_halt() -> None:
                cleared = clear_halt(settings.pg_conninfo)
                ui.notify("熔断已清除" if cleared else "当前没有熔断标志")

            ui.button("启动同步", on_click=start_watchlist).props("color=primary") \
                .tooltip("同步首页已添加的全部股票")
            ui.button("同步全市场指标", on_click=start_market).props(
                "color=primary outline"
            ).tooltip("单独执行下方市场级任务")
            ui.button("停止", on_click=stop_run).props("color=warning outline")
            ui.button("清除熔断", on_click=do_clear_halt).props("color=negative outline")

        # ── 当前进度 ──
        ui.label("当前进度").classes("text-lg font-bold mt-2")
        phase_label = ui.label("").classes("text-lg font-bold")
        progress = ui.linear_progress(value=0, show_value=False).classes("w-full")
        detail_label = ui.label("").classes("text-sm")
        stat_label = ui.label("").classes("text-sm text-gray-600")
        notes_log = ui.log(max_lines=10).classes("w-full h-32")

        errors_expand = ui.expansion("错误明细", icon="error_outline").classes("w-full")
        with errors_expand:
            errors_log = ui.log(max_lines=50).classes("w-full h-48")

        # ── 市场级任务（默认全市场指标，单独列出）──
        ui.label("市场级任务").classes("text-lg font-bold mt-2")
        ui.label(
            "全市场指标的默认同步任务，单独列出；「同步全市场指标」会全部执行，"
            "「启动同步」只自动带上交易日历与证券信息。"
        ).classes("text-sm text-gray-500")
        market_table = ui.table(
            columns=[
                {"name": "label", "label": "任务", "field": "label", "align": "left"},
                {"name": "dataset", "label": "数据集", "field": "dataset",
                 "align": "left"},
                {"name": "last_date", "label": "覆盖至", "field": "last_date",
                 "align": "left"},
                {"name": "last_synced_at", "label": "上次同步", "field": "last_synced_at",
                 "align": "left"},
            ],
            rows=[],
            row_key="dataset",
        ).classes("w-full")

        # ── 运行历史（点击行看任务详情）──
        ui.label("同步历史").classes("text-lg font-bold mt-2")
        ui.label("点击一行查看该任务的详细情况").classes("text-sm text-gray-500")
        runs_table = (
            ui.table(
                columns=[
                    {"name": "id", "label": "#", "field": "id", "align": "left"},
                    {"name": "kind", "label": "任务", "field": "kind", "align": "left"},
                    {"name": "started_at", "label": "开始", "field": "started_at",
                     "align": "left"},
                    {"name": "finished_at", "label": "结束", "field": "finished_at",
                     "align": "left"},
                    {"name": "status", "label": "状态", "field": "status",
                     "align": "left"},
                    {"name": "rows_total", "label": "行数", "field": "rows_total",
                     "align": "right"},
                    {"name": "codes", "label": "完成码数", "field": "codes",
                     "align": "right"},
                ],
                rows=[],
                row_key="id",
            )
            .classes("w-full cursor-pointer")
            .props("hover")
        )

        runs_cache: dict[int, dict] = {}

        def show_run_detail(run_id: int) -> None:
            run = runs_cache.get(run_id)
            if run is None:
                return
            params = run.get("params") or {}
            stats = run.get("stats") or {}
            with ui.dialog() as dialog, ui.card().classes("w-[640px] max-w-full"):
                ui.label(f"同步任务 #{run_id}").classes("text-lg font-bold")
                ui.label(_run_kind(params)).classes("text-sm text-gray-600")
                with ui.grid(columns=2).classes("w-full gap-x-8 gap-y-1 text-sm"):
                    ui.label("开始")
                    ui.label((run["started_at"] or "")[:19])
                    ui.label("结束")
                    ui.label((run["finished_at"] or "")[:19] or "—")
                    ui.label("状态")
                    ui.label(run["status"])
                    ui.label("完成码数")
                    ui.label(
                        f"{stats.get('done_codes', 0)}/{stats.get('total_codes', 0)}"
                    )
                    ui.label("切片数")
                    ui.label(str(stats.get("slices_done", 0)))

                rows_by = stats.get("rows_by_dataset") or {}
                if rows_by:
                    ui.label("各数据集入库行数").classes("font-bold mt-2")
                    with ui.grid(columns=2).classes("w-full gap-x-8 gap-y-1 text-sm"):
                        for ds, n in sorted(rows_by.items()):
                            ui.label(_label(ds))
                            ui.label(str(n))

                errors = stats.get("errors") or []
                if errors:
                    ui.label(f"错误（{len(errors)}）").classes(
                        "font-bold mt-2 text-red-700"
                    )
                    with ui.column().classes("w-full gap-0 text-sm max-h-48 overflow-auto"):
                        for err in errors:
                            where = "/".join(x for x in (err.get("code"),
                                                         err.get("dataset")) if x)
                            ui.label(f"{where}: {err.get('error')}")

                with ui.row().classes("w-full justify-end mt-2"):
                    ui.button("关闭", on_click=dialog.close).props("flat")
            dialog.open()

        runs_table.on("rowClick", lambda e: show_run_detail(e.args[1]["id"]))

        seen = {"notes": 0, "errors": 0}

        def refresh() -> None:
            runner = state.runner
            if runner is None:
                return
            st = runner.state()
            halt = _halt()
            if halt:
                halt_banner.text = f"⛔ 熔断中：{halt.get('reason', '?')}"
                halt_banner.classes(remove="hidden")
            else:
                halt_banner.classes(add="hidden")

            if st["running"]:
                phase_label.text = f"运行中 · {st['phase']}"
                total = st["code_total"] or 1
                progress.value = st["code_idx"] / total if st["code_total"] else 0
                detail_label.text = (
                    f"{st['current_code']} ({st['code_idx']}/{st['code_total']}) · "
                    f"{st['current_label']}"
                )
            else:
                phase_label.text = f"空闲（上次结果：{st['status'] or '无记录'}）"
                progress.value = 1.0 if st["status"] == "done" else 0
                detail_label.text = ""
            elapsed = ""
            if st["started_at"]:
                end = st["finished_at"] or time.time()
                elapsed = f" · 用时 {int(end - st['started_at'])}s"
            stat_label.text = (
                f"切片 {st['slices_done']} · 行 {st['rows_total']} · "
                f"调用 {st['calls_total']}（{st['calls_per_minute']}/min，限 "
                f"{settings.rate_limit_per_minute}）· 错误 {len(st['errors'])}{elapsed}"
            )
            for note in st["notes"][seen["notes"]:]:
                notes_log.push(note)
            seen["notes"] = len(st["notes"])
            for err in st["errors"][seen["errors"]:]:
                errors_log.push(f"{err['code']}/{err['dataset']}: {err['error']}")
            seen["errors"] = len(st["errors"])

            wms = queries.market_watermarks()
            market_table.rows = [
                {
                    "label": _label(ds),
                    "dataset": ds,
                    "last_date": (wms.get(ds) or {}).get("last_date") or "未同步",
                    "last_synced_at":
                        ((wms.get(ds) or {}).get("last_synced_at") or "")[:19] or "—",
                }
                for ds in _MARKET_DATASETS
            ]
            market_table.update()

            runs = queries.recent_runs(20)
            runs_cache.clear()
            runs_cache.update({r["id"]: r for r in runs})
            runs_table.rows = [
                {
                    "id": r["id"],
                    "kind": _run_kind(r.get("params") or {}),
                    "started_at": (r["started_at"] or "")[:19],
                    "finished_at": (r["finished_at"] or "")[:19],
                    "status": r["status"],
                    "rows_total": sum(
                        ((r.get("stats") or {}).get("rows_by_dataset") or {}).values()
                    ),
                    "codes": (r.get("stats") or {}).get("done_codes", ""),
                }
                for r in runs
            ]
            runs_table.update()

        def _halt() -> dict | None:
            import psycopg

            with psycopg.connect(settings.pg_conninfo) as conn:
                return read_halt(conn)

        ui.timer(1.0, refresh)


def _run_kind(params: dict) -> str:
    """把 RunParams 摘要成人话，用于历史列表与详情。"""
    codes = params.get("codes") or []
    datasets = params.get("datasets") or []
    if codes:
        head = "、".join(codes[:3]) + ("…" if len(codes) > 3 else "")
        what = f"指定股票（{head}）"
    elif params.get("watchlist_only"):
        what = "关注列表"
    else:
        what = "全市场"
    if datasets:
        if set(datasets) == set(_MARKET_DATASETS):
            return "全市场指标"
        ds = "、".join(_label(d) for d in datasets[:3]) + (
            "…" if len(datasets) > 3 else ""
        )
        return f"{what} · {ds}"
    return what
