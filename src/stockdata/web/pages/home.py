"""首页：添加/管理关注代码。添加不触发抓取——数据由同步任务灌入。"""

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
        ui.label("关注列表").classes("text-2xl font-bold")
        ui.label(
            "添加代码只登记关注，不触发抓取；数据由「同步」页或 CLI 启动的同步任务灌入。"
        ).classes("text-sm text-gray-500")

        with ui.row().classes("items-end gap-2"):
            code_input = ui.input(
                "股票代码", placeholder="sh.600000 / sz.000001"
            ).classes("w-56")
            note_input = ui.input("备注（可选）").classes("w-56")

            def add_code() -> None:
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
                code_input.value = ""
                note_input.value = ""
                watch_table.refresh()
                ui.notify(f"已添加 {code}", type="positive")

            ui.button("添加", on_click=add_code).props("color=primary")

        watch_table()


@ui.refreshable
def watch_table() -> None:
    rows = queries.watchlist_overview()
    if not rows:
        ui.label("还没有关注任何股票").classes("text-gray-400")
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
    table = ui.table(columns=columns, rows=table_rows, row_key="code").classes("w-full")
    table.add_slot(
        "body-cell-actions",
        """
        <q-td :props="props" class="text-right">
            <q-btn dense flat color="primary" label="看图"
                   :href="'/chart/' + props.row.code" />
            <q-btn dense flat color="secondary" label="同步"
                   @click="$parent.$emit('sync', props.row.code)" />
            <q-btn dense flat color="negative" label="移除"
                   @click="$parent.$emit('remove', props.row.code)" />
        </q-td>
        """,
    )
    table.on("sync", lambda e: sync_one(e.args))
    table.on("remove", lambda e: remove(e.args))
