"""同步仪表盘页：/sync。与 CLI TUI 展示同一份 RunState（ui.timer 轮询）。"""

from __future__ import annotations

import time

from nicegui import ui

from stockdata.config import settings
from stockdata.db import queries
from stockdata.sync.engine import RunParams, clear_halt, read_halt

from .. import state
from .common import nav


@ui.page("/sync")
def sync_page() -> None:
    nav("sync")
    with ui.column().classes("w-full max-w-5xl mx-auto p-4 gap-4"):
        ui.label("同步").classes("text-2xl font-bold")

        halt_banner = ui.label("").classes(
            "w-full p-2 rounded bg-red-100 text-red-800 font-bold hidden"
        )

        # ── 控制区 ──
        with ui.row().classes("items-end gap-2"):
            watchlist_only = ui.checkbox("只同步关注列表", value=True)
            datasets_input = ui.input(
                "数据集过滤（可选）", placeholder="k_d,k_5（留空=全部）"
            ).classes("w-64")

            def start_run() -> None:
                runner = state.get_runner()
                datasets = [d.strip() for d in (datasets_input.value or "").split(",")
                            if d.strip()]
                ok, msg = (False, "处于熔断状态，先清除") if _halt() else runner.start(
                    RunParams(datasets=datasets, watchlist_only=watchlist_only.value)
                )
                ui.notify(msg if ok else f"未启动：{msg}",
                          type="positive" if ok else "warning")

            def stop_run() -> None:
                if state.get_runner().stop():
                    ui.notify("已请求停止（完成当前切片后退出）")
                else:
                    ui.notify("当前没有在运行的任务", type="info")

            def do_clear_halt() -> None:
                cleared = clear_halt(settings.pg_conninfo)
                ui.notify("熔断已清除" if cleared else "当前没有熔断标志")

            ui.button("启动同步", on_click=start_run).props("color=primary")
            ui.button("停止", on_click=stop_run).props("color=warning outline")
            ui.button("清除熔断", on_click=do_clear_halt).props("color=negative outline")

        # ── 进度区 ──
        phase_label = ui.label("").classes("text-lg font-bold")
        progress = ui.linear_progress(value=0, show_value=False).classes("w-full")
        detail_label = ui.label("").classes("text-sm")
        stat_label = ui.label("").classes("text-sm text-gray-600")
        notes_log = ui.log(max_lines=10).classes("w-full h-32")

        errors_expand = ui.expansion("错误明细", icon="error_outline").classes("w-full")
        with errors_expand:
            errors_log = ui.log(max_lines=50).classes("w-full h-48")

        # ── 运行历史 ──
        ui.label("运行历史").classes("text-lg font-bold mt-2")
        runs_table = ui.table(
            columns=[
                {"name": "id", "label": "#", "field": "id", "align": "left"},
                {"name": "started_at", "label": "开始", "field": "started_at", "align": "left"},
                {"name": "finished_at", "label": "结束", "field": "finished_at", "align": "left"},
                {"name": "status", "label": "状态", "field": "status", "align": "left"},
                {"name": "codes", "label": "完成码数", "field": "codes", "align": "left"},
            ],
            rows=[],
            row_key="id",
        ).classes("w-full")

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

            runs_table.rows = [
                {
                    "id": r["id"],
                    "started_at": (r["started_at"] or "")[:19],
                    "finished_at": (r["finished_at"] or "")[:19],
                    "status": r["status"],
                    "codes": (r["stats"] or {}).get("done_codes", ""),
                }
                for r in queries.recent_runs(10)
            ]
            runs_table.update()

        def _halt() -> dict | None:
            import psycopg

            with psycopg.connect(settings.pg_conninfo) as conn:
                return read_halt(conn)

        ui.timer(1.0, refresh)
