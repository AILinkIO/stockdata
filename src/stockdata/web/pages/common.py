"""页面公共件：导航栏。"""

from __future__ import annotations

from nicegui import ui


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
