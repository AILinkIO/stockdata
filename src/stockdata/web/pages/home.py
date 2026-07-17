"""首页：关注列表。添加代码走弹窗，仅登记关注不触发抓取；点击行进入 K 线页看完整数据。"""

from __future__ import annotations

import re

import httpx
from nicegui import ui

from stockdata.config import settings
from stockdata.db import queries

from .common import nav

_CODE_RE = re.compile(r"^(sh|sz)\.\d{6}$")


@ui.page("/")
def home_page() -> None:
    nav("home")
    with ui.column().classes("w-full max-w-4xl mx-auto p-4 gap-4"):
        with ui.row().classes("w-full items-center justify-between"):
            ui.label("关注列表").classes("text-2xl font-bold")
            ui.button("添加股票", icon="add", on_click=_open_add_dialog).props(
                "color=primary"
            )
        ui.label(
            "添加代码只登记关注，不触发抓取；数据由「同步」页或 CLI 启动的同步任务灌入。"
            "点击行可查看 K 线与完整历史数据。"
        ).classes("text-sm text-gray-500")

        watch_table()


def _open_add_dialog() -> None:
    with ui.dialog() as dialog, ui.card().classes("w-96"):
        ui.label("添加股票").classes("text-lg font-bold")
        code_input = ui.input(
            "股票代码", placeholder="sh.600000 / sz.000001"
        ).classes("w-full")
        note_input = ui.input("备注（可选）").classes("w-full")

        def submit() -> None:
            code = (code_input.value or "").strip().lower()
            if not _CODE_RE.match(code):
                ui.notify("代码格式：sh.600000 / sz.000001", type="warning")
                return
            if not queries.security_exists(code):
                ui.notify(
                    f"{code} 不在证券表中（可能证券列表还没同步过），已照加",
                    type="info",
                )
            queries.add_watch(code, note_input.value or "")
            dialog.close()
            watch_table.refresh()
            ui.notify(f"已添加 {code}", type="positive")

        code_input.on("keydown.enter", submit)
        note_input.on("keydown.enter", submit)
        with ui.row().classes("w-full justify-end gap-2 mt-2"):
            ui.button("取消", on_click=dialog.close).props("flat")
            ui.button("添加", on_click=submit).props("color=primary")
    dialog.open()


@ui.refreshable
def watch_table() -> None:
    rows = queries.watchlist_overview()
    if not rows:
        ui.label("还没有关注任何股票，点右上「添加股票」开始").classes("text-gray-400")
        return

    def sync_one(code: str) -> None:
        try:
            resp = httpx.post(
                f"http://127.0.0.1:{settings.web_port}/api/sync/run",
                json={"codes": [code]},
                timeout=5,
            )
            if resp.status_code == 202:
                ui.notify(f"已启动 {code} 的同步，进度见「同步」页", type="positive")
            else:
                ui.notify(resp.json().get("detail", "启动失败"), type="warning")
        except httpx.HTTPError as e:
            ui.notify(f"请求失败: {e}", type="negative")

    def remove(code: str) -> None:
        queries.remove_watch(code)
        watch_table.refresh()
        ui.notify(f"已移除 {code}")

    columns = [
        {"name": "code", "label": "代码", "field": "code", "align": "left"},
        {"name": "code_name", "label": "名称", "field": "code_name", "align": "left"},
        {"name": "k_d_until", "label": "日K水位", "field": "k_d_until", "align": "left"},
        {"name": "k_5_until", "label": "5分水位", "field": "k_5_until", "align": "left"},
        {"name": "actions", "label": "", "field": "code"},
    ]
    table_rows = [
        {
            "code": r["code"],
            "code_name": r["code_name"] or "—",
            "k_d_until": str(r["k_d_until"] or "未同步"),
            "k_5_until": str(r["k_5_until"] or "未同步"),
        }
        for r in rows
    ]
    table = (
        ui.table(columns=columns, rows=table_rows, row_key="code")
        .classes("w-full cursor-pointer")
        .props("hover")
    )
    table.add_slot(
        "body-cell-actions",
        """
        <q-td :props="props" class="text-right">
            <q-btn dense flat color="secondary" label="同步"
                   @click.stop="$parent.$emit('sync', props.row.code)" />
            <q-btn dense flat color="negative" label="移除"
                   @click.stop="$parent.$emit('remove', props.row.code)" />
        </q-td>
        """,
    )
    table.on("sync", lambda e: sync_one(e.args))
    table.on("remove", lambda e: remove(e.args))
    table.on("rowClick", lambda e: ui.navigate.to(f"/chart/{e.args[1]['code']}"))
