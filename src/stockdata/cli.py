"""stockdata CLI（Typer）。

- db init/reset：schema 管理（直连 PG）。
- serve：启动 NiceGUI 单服务（Web + 同步 worker）。
- sync run/status/stop/clear-halt：HTTP 薄客户端，连常驻服务的 /api/sync/*，
  同步进度既可在终端（--tui rich 仪表 / --plain 日志行）也可在 Web /sync 页查看。
"""

from __future__ import annotations

import typer

from stockdata.config import settings

app = typer.Typer(no_args_is_help=True, help="A 股数据同步与展示")
db_app = typer.Typer(no_args_is_help=True, help="数据库 schema 管理")
sync_app = typer.Typer(no_args_is_help=True, help="同步任务（连常驻服务）")
app.add_typer(db_app, name="db")
app.add_typer(sync_app, name="sync")


@db_app.command("init")
def db_init() -> None:
    """创建/补齐全部表（幂等）。"""
    from stockdata.db.init import init_schema

    init_schema(settings.pg_conninfo)
    typer.echo("schema 初始化完成")


@db_app.command("reset")
def db_reset(
    yes: bool = typer.Option(False, "--yes", help="确认删除 public 下全部表"),
) -> None:
    """删除 public schema 下全部表（含旧 dotnet 表）并重建 schema。"""
    from stockdata.db.init import init_schema, list_tables, reset_db

    tables = list_tables(settings.pg_conninfo)
    if not tables:
        typer.echo("库中无表，直接初始化")
    else:
        typer.echo(f"将删除 {len(tables)} 张表: {', '.join(tables)}")
        if not yes and not typer.confirm("确认删除？"):
            raise typer.Abort()
        reset_db(settings.pg_conninfo)
        typer.echo("旧表已全部删除")
    init_schema(settings.pg_conninfo)
    typer.echo("新 schema 初始化完成")


@app.command("serve")
def serve() -> None:
    """启动 NiceGUI 单服务（Web 页面 + HTTP API + 同步 worker 线程）。"""
    from stockdata.web.app import run_app

    run_app()


@sync_app.command("run")
def sync_run(
    codes: str = typer.Option("", "--codes", help="逗号分隔的代码列表，如 sh.600000,sz.000001"),
    datasets: str = typer.Option("", "--datasets", help="逗号分隔的数据集过滤，如 k_d,k_5"),
    watchlist_only: bool = typer.Option(False, "--watchlist-only", help="只同步关注列表"),
    tui: bool = typer.Option(None, "--tui/--plain", help="rich TUI / 纯日志行（默认按 TTY 自动）"),
    attach: bool = typer.Option(False, "--attach", help="已有任务在跑时只附加观看进度"),
) -> None:
    """启动一次同步并跟踪进度（服务须已运行：stockdata serve / docker compose up）。"""
    from stockdata.client import run_and_follow

    run_and_follow(
        base=settings.app_base,
        codes=[c.strip() for c in codes.split(",") if c.strip()],
        datasets=[d.strip() for d in datasets.split(",") if d.strip()],
        watchlist_only=watchlist_only,
        tui=tui,
        attach=attach,
    )


@sync_app.command("status")
def sync_status() -> None:
    """查看当前运行状态与各水位概览。"""
    from stockdata.client import show_status

    show_status(settings.app_base)


@sync_app.command("stop")
def sync_stop() -> None:
    """请求停止当前同步（完成当前切片后干净退出）。"""
    from stockdata.client import stop_run

    stop_run(settings.app_base)


@sync_app.command("clear-halt")
def sync_clear_halt() -> None:
    """清除拉黑熔断标志（确认解封后手动执行）。"""
    from stockdata.client import clear_halt

    clear_halt(settings.app_base)


if __name__ == "__main__":
    app()
