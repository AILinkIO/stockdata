"""同步引擎：单连接、严格串行地执行一次 run。

- 每切片一个事务：写数据 + 推水位原子提交 → 任意时刻中断都能片级续传。
- DataSourceError 指数退避重试（可被 stop 事件打断）；耗尽记错误继续下一数据集。
- BlacklistError → 持久化 halt（sync_state）→ 整个 run 立即终止，
  后续 run 拒绝启动直到 clear-halt。
- PG advisory lock 防多实例并发驱动 baostock。
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field
from datetime import date
from typing import Sequence

import psycopg

from stockdata.config import Settings
from stockdata.provider.interface import BlacklistError, DataSourceError, Provider

from . import watermark
from .datasets import CODE_HANDLERS, MARKET_HANDLERS, MINUTE_HANDLERS, Dataset
from .planner import PlanContext, Slice

logger = logging.getLogger(__name__)

ADVISORY_LOCK_KEY = "stockdata_sync"


class HaltError(Exception):
    """拉黑熔断：run 终止并持久化 halt 标志。"""


class StopRequested(Exception):
    """用户请求停止：完成当前切片后干净退出。"""


class AnotherRunActive(Exception):
    """advisory lock 被占：另一进程/实例正在驱动 baostock。"""


@dataclass
class RunParams:
    codes: list[str] = field(default_factory=list)      # 空=全市场
    datasets: list[str] = field(default_factory=list)   # 空=全部数据集
    watchlist_only: bool = False

    def to_json(self) -> dict:
        return {
            "codes": self.codes, "datasets": self.datasets,
            "watchlist_only": self.watchlist_only,
        }


@dataclass
class RunStats:
    total_codes: int = 0
    done_codes: int = 0
    rows_by_dataset: dict[str, int] = field(default_factory=dict)
    slices_done: int = 0
    errors: list[dict] = field(default_factory=list)   # {code, dataset, error}
    status: str = "running"                            # done/stopped/halted/failed

    def add_rows(self, dataset: str, rows: int) -> None:
        self.rows_by_dataset[dataset] = self.rows_by_dataset.get(dataset, 0) + rows

    def to_json(self) -> dict:
        return {
            "total_codes": self.total_codes,
            "done_codes": self.done_codes,
            "rows_by_dataset": self.rows_by_dataset,
            "slices_done": self.slices_done,
            "errors": self.errors[-50:],  # 只留尾部，防膨胀
            "status": self.status,
        }


class EngineEvents:
    """进度回调（Runner 覆盖实现；默认 no-op）。"""

    def phase(self, name: str) -> None: ...

    def code_start(self, code: str, idx: int, total: int) -> None: ...

    def slice_done(self, code: str, dataset: str, label: str, rows: int) -> None: ...

    def dataset_error(self, code: str, dataset: str, error: str) -> None: ...

    def note(self, msg: str) -> None: ...


def read_halt(conn: psycopg.Connection) -> dict | None:
    row = conn.execute("SELECT value FROM sync_state WHERE key = 'halt'").fetchone()
    return row[0] if row else None


def clear_halt(conninfo: str) -> bool:
    with psycopg.connect(conninfo) as conn:
        cur = conn.execute("DELETE FROM sync_state WHERE key = 'halt'")
        return cur.rowcount > 0


class SyncEngine:
    def __init__(
        self,
        conninfo: str,
        provider: Provider,
        settings: Settings,
        events: EngineEvents | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        self._conninfo = conninfo
        self._provider = provider
        self._settings = settings
        self._events = events or EngineEvents()
        self._stop = stop_event or threading.Event()
        self.run_id: int | None = None

    # ── 入口 ──

    def run(self, params: RunParams, today: date | None = None) -> RunStats:
        stats = RunStats()
        today = today or date.today()
        with psycopg.connect(self._conninfo, autocommit=True) as conn:
            locked = conn.execute(
                "SELECT pg_try_advisory_lock(hashtext(%s))", (ADVISORY_LOCK_KEY,)
            ).fetchone()[0]
            if not locked:
                raise AnotherRunActive("另一个同步实例持有 advisory lock")
            halt = read_halt(conn)
            if halt:
                raise HaltError(f"处于熔断状态（{halt.get('reason', '?')}），先 clear-halt")

            self.run_id = conn.execute(
                "INSERT INTO sync_run (params) VALUES (%s) RETURNING id",
                (json.dumps(params.to_json()),),
            ).fetchone()[0]
            try:
                self._run_inner(conn, params, stats, today)
                stats.status = "done"
            except StopRequested:
                stats.status = "stopped"
                self._events.note("已按请求停止")
            except HaltError as e:
                stats.status = "halted"
                self._persist_halt(conn, str(e))
                self._events.note(f"熔断停机: {e}")
            except AnotherRunActive:
                raise
            except Exception as e:
                stats.status = "failed"
                stats.errors.append({"code": "", "dataset": "", "error": f"run 失败: {e}"})
                logger.exception("run 异常终止")
            finally:
                conn.execute(
                    "UPDATE sync_run SET finished_at = now(), status = %s, stats = %s "
                    "WHERE id = %s",
                    (stats.status, json.dumps(stats.to_json()), self.run_id),
                )
        return stats

    # ── 主流程 ──

    def _run_inner(
        self, conn: psycopg.Connection, params: RunParams, stats: RunStats, today: date
    ) -> None:
        wanted = set(params.datasets) if params.datasets else None

        def keep(handler) -> bool:
            return wanted is None or handler.dataset.value in wanted

        # 市场级阶段（指定了 codes/watchlist 且未显式点名市场数据集时跳过，
        # 但交易日历除外——按码规划依赖它）
        market = [h for h in MARKET_HANDLERS if keep(h)]
        explicit_scope = bool(params.codes or params.watchlist_only)
        if explicit_scope and wanted is None:
            market = [h for h in MARKET_HANDLERS
                      if h.dataset in (Dataset.TRADE_CALENDAR, Dataset.SECURITY)]
        if market:
            self._events.phase("市场级数据")
            for handler in market:
                self._check_stop()
                self._run_dataset(conn, handler, "", None, stats, today)

        codes = self._resolve_codes(conn, params)
        stats.total_codes = len(codes)

        code_handlers = [h for h in CODE_HANDLERS if keep(h)]
        minute_handlers = [h for h in MINUTE_HANDLERS if keep(h)]

        if code_handlers:
            self._events.phase("按码同步（日频）")
            for idx, (code, ipo) in enumerate(codes):
                self._check_stop()
                self._events.code_start(code, idx + 1, len(codes))
                for handler in code_handlers:
                    self._check_stop()
                    self._run_dataset(conn, handler, code, ipo, stats, today)
                stats.done_codes += 1
                self._flush_stats(conn, stats)

        if minute_handlers:
            self._events.phase("按码同步（分钟线）")
            for idx, (code, ipo) in enumerate(codes):
                self._check_stop()
                self._events.code_start(code, idx + 1, len(codes))
                for handler in minute_handlers:
                    self._check_stop()
                    self._run_dataset(conn, handler, code, ipo, stats, today)
                self._flush_stats(conn, stats)

    def _resolve_codes(
        self, conn: psycopg.Connection, params: RunParams
    ) -> list[tuple[str, date | None]]:
        if params.codes:
            rows = conn.execute(
                "SELECT code, ipo_date FROM security WHERE code = ANY(%s)",
                (params.codes,),
            ).fetchall()
            known = {r[0]: r[1] for r in rows}
            return [(c, known.get(c)) for c in params.codes]
        if params.watchlist_only:
            rows = conn.execute(
                "SELECT w.code, s.ipo_date FROM watchlist w "
                "LEFT JOIN security s ON s.code = w.code ORDER BY w.code"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT code, ipo_date FROM security "
                "WHERE type = 1 AND status = 1 ORDER BY code"
            ).fetchall()
        return [(r[0], r[1]) for r in rows]

    # ── 数据集/切片执行 ──

    def _run_dataset(
        self,
        conn: psycopg.Connection,
        handler,
        code: str,
        ipo: date | None,
        stats: RunStats,
        today: date,
    ) -> None:
        dataset = handler.dataset.value
        ctx = PlanContext(
            conn=conn, provider=self._provider, settings=self._settings,
            today=today, code=code, ipo_date=ipo,
            wm=watermark.get(conn, code, dataset),
        )
        try:
            slices = handler.plan(ctx)
        except Exception as e:
            stats.errors.append({"code": code, "dataset": dataset, "error": f"plan: {e}"})
            self._events.dataset_error(code, dataset, str(e))
            logger.exception("plan 失败 %s/%s", code, dataset)
            return

        for sl in slices:
            self._check_stop()
            try:
                self._run_slice_with_retry(conn, ctx, handler, sl, stats)
            except (HaltError, StopRequested):
                raise
            except Exception as e:
                stats.errors.append({"code": code, "dataset": dataset, "error": str(e)})
                self._events.dataset_error(code, dataset, str(e))
                logger.warning("数据集失败 %s/%s: %s", code, dataset, e)
                return  # 本数据集放弃（水位未推进，下次续），继续下一数据集

    def _run_slice_with_retry(
        self,
        conn: psycopg.Connection,
        ctx: PlanContext,
        handler,
        sl: Slice,
        stats: RunStats,
    ) -> None:
        dataset = handler.dataset.value
        attempt = 0
        while True:
            try:
                with conn.transaction():
                    result = handler.run_slice(ctx, sl)
                    last = result.actual_last if result.rows else sl.empty_advance_to
                    watermark.advance(conn, ctx.code, dataset, result.actual_first, last)
                stats.slices_done += 1
                stats.add_rows(dataset, result.rows)
                self._events.slice_done(ctx.code, dataset, sl.label, result.rows)
                # 水位推进后刷新 ctx.wm，供同数据集后续切片使用
                ctx.wm = watermark.get(conn, ctx.code, dataset)
                return
            except BlacklistError as e:
                raise HaltError(str(e)) from e
            except DataSourceError as e:
                attempt += 1
                if attempt > self._settings.max_retries:
                    raise
                backoff = min(
                    self._settings.retry_base_seconds * 2 ** (attempt - 1),
                    self._settings.retry_max_backoff_seconds,
                )
                self._events.note(
                    f"{sl.label}: 第 {attempt}/{self._settings.max_retries} 次重试，"
                    f"退避 {backoff}s（{e}）"
                )
                if self._stop.wait(backoff):
                    raise StopRequested() from e

    # ── 辅助 ──

    def _check_stop(self) -> None:
        if self._stop.is_set():
            raise StopRequested()

    def _persist_halt(self, conn: psycopg.Connection, reason: str) -> None:
        conn.execute(
            """
            INSERT INTO sync_state (key, value) VALUES ('halt', %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
            """,
            (json.dumps({"reason": reason}),),
        )

    def _flush_stats(self, conn: psycopg.Connection, stats: RunStats) -> None:
        conn.execute(
            "UPDATE sync_run SET stats = %s WHERE id = %s",
            (json.dumps(stats.to_json()), self.run_id),
        )


def resolve_code_list(conninfo: str, params: RunParams) -> Sequence[str]:
    """供 API 预检查用：返回将要同步的 code 列表（不加锁）。"""
    engine_conn = psycopg.connect(conninfo)
    try:
        dummy = SyncEngine.__new__(SyncEngine)
        return [c for c, _ in dummy._resolve_codes(engine_conn, params)]
    finally:
        engine_conn.close()
