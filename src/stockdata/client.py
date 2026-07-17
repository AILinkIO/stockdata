"""CLI 薄客户端：通过常驻服务的 /api/sync/* 启动任务、跟踪进度。

同步引擎跑在服务进程里（唯一 baostock worker）；本模块只做展示：
--tui 用 rich Live 仪表，--plain 输出逐行日志（cron 友好）。
"""

from __future__ import annotations

import sys
import time

import httpx
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

console = Console()
err_console = Console(stderr=True)


def _client(base: str) -> httpx.Client:
    return httpx.Client(base_url=base, timeout=10)


def _die_no_service(base: str) -> None:
    err_console.print(
        f"[red]无法连接服务 {base}[/red] —— 请先启动：`stockdata serve` 或 `./up.sh`"
    )
    raise SystemExit(2)


def run_and_follow(
    base: str,
    codes: list[str],
    datasets: list[str],
    watchlist_only: bool,
    tui: bool | None,
    attach: bool,
) -> None:
    if tui is None:
        tui = sys.stdout.isatty()
    with _client(base) as client:
        try:
            resp = client.post("/api/sync/run", json={
                "codes": codes, "datasets": datasets, "watchlist_only": watchlist_only,
            })
        except httpx.TransportError:
            _die_no_service(base)
        if resp.status_code == 409:
            detail = resp.json().get("detail", "")
            if attach and "在运行" in detail:
                console.print("[yellow]已有任务在运行，附加观看进度[/yellow]")
            else:
                err_console.print(f"[red]未启动：{detail}[/red]（可用 --attach 观看现有任务）")
                raise SystemExit(1)
        elif resp.status_code != 202:
            err_console.print(f"[red]启动失败 HTTP {resp.status_code}: {resp.text}[/red]")
            raise SystemExit(1)
        else:
            console.print("[green]同步已启动[/green]")
        try:
            _follow(client, tui)
        except KeyboardInterrupt:
            console.print(
                "\n[yellow]已退出观看；同步仍在服务端运行。"
                "停止请用 `stockdata sync stop`。[/yellow]"
            )


def _poll(client: httpx.Client) -> dict:
    resp = client.get("/api/sync/status")
    resp.raise_for_status()
    return resp.json()


def _follow(client: httpx.Client, tui: bool) -> None:
    # 等 worker 真正进入 running（start 是异步交接）
    deadline = time.time() + 5
    data = _poll(client)
    while not data["state"]["running"] and time.time() < deadline:
        time.sleep(0.3)
        data = _poll(client)
    if tui:
        _follow_tui(client, data)
    else:
        _follow_plain(client, data)
    final = _poll(client)
    status = final["state"]["status"]
    style = {"done": "green", "stopped": "yellow"}.get(status, "red")
    console.print(f"[{style}]同步结束：{status}[/{style}]")
    if final.get("halt"):
        err_console.print(f"[red]⛔ 熔断：{final['halt'].get('reason')}[/red]")
        raise SystemExit(3)
    if status not in ("done", "stopped"):
        raise SystemExit(1)


def _follow_tui(client: httpx.Client, data: dict) -> None:
    progress = Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        TimeElapsedColumn(),
    )
    task_id = progress.add_task("同步", total=None)

    def render(st: dict) -> Group:
        info = Table.grid(padding=(0, 2))
        info.add_row("阶段", st["phase"] or "—")
        info.add_row(
            "当前",
            f"{st['current_code']} ({st['code_idx']}/{st['code_total']})"
            if st["current_code"] else "—",
        )
        info.add_row("切片", st["current_label"] or "—")
        info.add_row(
            "统计",
            f"切片 {st['slices_done']} · 行 {st['rows_total']} · "
            f"调用 {st['calls_total']}（{st['calls_per_minute']}/min）· "
            f"错误 {len(st['errors'])}",
        )
        if st["notes"]:
            info.add_row("消息", st["notes"][-1])
        if st["errors"]:
            e = st["errors"][-1]
            info.add_row("最近错误", f"[red]{e['code']}/{e['dataset']}: {e['error'][:80]}[/red]")
        return Group(Panel(info, title="stockdata 同步"), progress)

    with Live(render(data["state"]), console=console, refresh_per_second=4) as live:
        while True:
            st = data["state"]
            if st["code_total"]:
                progress.update(
                    task_id, total=st["code_total"], completed=st["code_idx"],
                    description=st["phase"] or "同步",
                )
            live.update(render(st))
            if not st["running"]:
                break
            time.sleep(1)
            data = _poll(client)


def _follow_plain(client: httpx.Client, data: dict) -> None:
    last_slices = -1
    while True:
        st = data["state"]
        if st["slices_done"] != last_slices:
            last_slices = st["slices_done"]
            console.print(
                f"[{time.strftime('%H:%M:%S')}] {st['phase']} "
                f"{st['current_code']}({st['code_idx']}/{st['code_total']}) "
                f"{st['current_label']} | 切片{st['slices_done']} 行{st['rows_total']} "
                f"调用{st['calls_total']} 错误{len(st['errors'])}"
            )
        if not st["running"]:
            break
        time.sleep(2)
        data = _poll(client)


def show_status(base: str) -> None:
    with _client(base) as client:
        try:
            data = _poll(client)
            overview = client.get("/api/sync/overview").json()
        except httpx.TransportError:
            _die_no_service(base)
    st = data["state"]
    console.print(Panel(
        f"运行中：{st['running']} · 上次结果：{st['status'] or '无'} · "
        f"调用 {st['calls_total']}（{st['calls_per_minute']}/min）",
        title="服务状态",
    ))
    if data.get("halt"):
        err_console.print(f"[red]⛔ 熔断：{data['halt'].get('reason')}[/red]")

    wm = overview["watermarks"]
    table = Table(title=f"水位概览（在市股票 {wm['total_active_codes']}）")
    for col in ("数据集", "覆盖码数", "最旧水位", "最新水位", "上次同步"):
        table.add_column(col)
    for d in wm["datasets"]:
        table.add_row(
            d["dataset"], str(d["codes"]), str(d["min_last"] or "—"),
            str(d["max_last"] or "—"),
            (d["last_synced_at"] or "—")[:19],
        )
    console.print(table)

    runs = Table(title="最近运行")
    for col in ("#", "开始", "结束", "状态", "完成码数"):
        runs.add_column(col)
    for r in overview["runs"]:
        runs.add_row(
            str(r["id"]), (r["started_at"] or "")[:19], (r["finished_at"] or "")[:19],
            r["status"], str((r["stats"] or {}).get("done_codes", "")),
        )
    console.print(runs)


def stop_run(base: str) -> None:
    with _client(base) as client:
        try:
            resp = client.post("/api/sync/stop")
        except httpx.TransportError:
            _die_no_service(base)
    if resp.json().get("stopping"):
        console.print("[yellow]已请求停止（完成当前切片后退出）[/yellow]")
    else:
        console.print("当前没有在运行的任务")


def clear_halt(base: str) -> None:
    with _client(base) as client:
        try:
            resp = client.post("/api/sync/clear-halt")
        except httpx.TransportError:
            _die_no_service(base)
    if resp.json().get("cleared"):
        console.print("[green]熔断标志已清除[/green]")
    else:
        console.print("当前没有熔断标志")
