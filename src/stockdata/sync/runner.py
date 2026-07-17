"""SyncRunner：常驻 worker 线程 + 线程安全 RunState 快照。

整个进程唯一允许驱动 baostock 的地方。Web 页面与 CLI 都通过
start()/stop()/state() 这一套接口启动任务、观察进度（同一份 RunState）。
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from stockdata.config import Settings
from stockdata.provider.interface import BlacklistError, Provider

from .engine import (
    AnotherRunActive, EngineEvents, RunParams, SyncEngine,
    clear_halt, read_halt, save_halt,
)

logger = logging.getLogger(__name__)


@dataclass
class RunState:
    """当前/上一次 run 的进度快照（可 JSON 化）。"""

    run_id: int | None = None
    running: bool = False
    phase: str = ""
    current_code: str = ""
    code_idx: int = 0
    code_total: int = 0
    current_label: str = ""
    slices_done: int = 0
    rows_total: int = 0
    rows_by_dataset: dict[str, int] = field(default_factory=dict)
    errors: list[dict] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    calls_per_minute: int = 0
    calls_total: int = 0
    status: str = ""          # 上次结束状态 done/stopped/halted/failed
    started_at: float | None = None
    finished_at: float | None = None

    def to_json(self) -> dict[str, Any]:
        d = asdict(self)
        d["errors"] = self.errors[-20:]
        d["notes"] = self.notes[-10:]
        return d


class _StateEvents(EngineEvents):
    """引擎回调 → RunState（加锁更新）。"""

    def __init__(self, runner: "SyncRunner") -> None:
        self._r = runner

    def phase(self, name: str) -> None:
        with self._r._lock:
            self._r._state.phase = name
            self._r._state.current_code = ""
            self._r._state.current_label = ""

    def code_start(self, code: str, idx: int, total: int) -> None:
        with self._r._lock:
            st = self._r._state
            st.current_code, st.code_idx, st.code_total = code, idx, total

    def slice_done(self, code: str, dataset: str, label: str, rows: int) -> None:
        with self._r._lock:
            st = self._r._state
            st.current_label = label
            st.slices_done += 1
            st.rows_total += rows
            st.rows_by_dataset[dataset] = st.rows_by_dataset.get(dataset, 0) + rows

    def dataset_error(self, code: str, dataset: str, error: str) -> None:
        with self._r._lock:
            self._r._state.errors.append(
                {"code": code, "dataset": dataset, "error": error[:500]}
            )

    def note(self, msg: str) -> None:
        logger.info("%s", msg)
        with self._r._lock:
            self._r._state.notes.append(msg)
            del self._r._state.notes[:-50]


class SyncRunner:
    def __init__(self, conninfo: str, provider: Provider, settings: Settings) -> None:
        self._conninfo = conninfo
        self._provider = provider
        self._settings = settings
        self._lock = threading.Lock()
        self._state = RunState()
        self._stop_event = threading.Event()
        self._shutdown = threading.Event()
        self._pending: RunParams | None = None
        self._wakeup = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="sync-worker", daemon=True)
        self._thread.start()

    # ── 控制面 ──

    def start(self, params: RunParams) -> tuple[bool, str]:
        """请求启动一次 run。已在跑返回 (False, reason)。"""
        with self._lock:
            if self._state.running or self._pending is not None:
                return False, "已有同步任务在运行"
            self._pending = params
            self._stop_event.clear()
        self._wakeup.set()
        return True, "已启动"

    def stop(self) -> bool:
        """请求停止：完成当前切片后干净退出。"""
        with self._lock:
            running = self._state.running
        if running:
            self._stop_event.set()
        return running

    def state(self) -> dict:
        with self._lock:
            st = self._state.to_json()
        limiter = getattr(self._provider, "rate_limiter", None)
        if limiter is not None:
            st["calls_per_minute"] = limiter.current_rate()
            st["calls_total"] = limiter.total_acquired
        return st

    def shutdown(self) -> None:
        """进程关停：停止当前 run、结束线程、干净登出 baostock。"""
        self._shutdown.set()
        self._stop_event.set()
        self._wakeup.set()
        self._thread.join(timeout=30)
        try:
            self._provider.logout()
        except Exception:
            pass

    # ── worker 线程 ──

    def _loop(self) -> None:
        while not self._shutdown.is_set():
            # 空闲等待启动请求；顺带做空闲登出与熔断探测检查
            if not self._wakeup.wait(timeout=5):
                self._maybe_idle_logout()
                self._maybe_probe_halt()
                self._maybe_log_lag()
                continue
            self._wakeup.clear()
            with self._lock:
                params = self._pending
                self._pending = None
            if params is None or self._shutdown.is_set():
                continue
            self._run_once(params)

    def _run_once(self, params: RunParams) -> None:
        with self._lock:
            self._state = RunState(running=True, started_at=time.time())
        events = _StateEvents(self)
        engine = SyncEngine(
            self._conninfo, self._provider, self._settings,
            events=events, stop_event=self._stop_event,
        )
        try:
            stats = engine.run(params)
            status = stats.status
        except AnotherRunActive as e:
            status = "failed"
            events.note(f"启动失败: {e}")
        except Exception as e:  # HaltError 已在引擎内转为 stats；这里兜底未知异常
            status = "failed"
            events.note(f"run 异常: {e}")
            logger.exception("run 异常")
        if status == "stopped" and self._shutdown.is_set() and engine.run_id:
            # 进程关停打断（非用户主动 stop）→ interrupted，下次启动自动续跑
            status = "interrupted"
            try:
                import psycopg

                with psycopg.connect(self._conninfo) as conn:
                    conn.execute(
                        "UPDATE sync_run SET status = 'interrupted' WHERE id = %s",
                        (engine.run_id,),
                    )
            except Exception:
                logger.exception("标记 interrupted 失败")
        with self._lock:
            self._state.running = False
            self._state.status = status
            self._state.run_id = engine.run_id
            self._state.finished_at = time.time()

    # ── 熔断自动探测（仅 login_error；拉黑绝不自动探测）──

    def _maybe_probe_halt(self) -> None:
        hours = getattr(self._settings, "halt_probe_interval_hours", 0)
        if hours <= 0:
            return
        now = time.monotonic()
        if now < getattr(self, "_next_halt_check", 0):
            return
        self._next_halt_check = now + 60  # DB 检查本身限频 1 次/分钟
        try:
            self._probe_halt(hours)
        except Exception:
            logger.exception("熔断探测异常")

    def _probe_halt(self, hours: float) -> None:
        import psycopg

        with psycopg.connect(self._conninfo) as conn:
            halt = read_halt(conn)
        if not halt or halt.get("kind") != "login_error":
            return
        interval = hours * 3600
        ref = halt.get("last_probe_at") or halt.get("halted_at")
        if ref:
            try:
                elapsed = (
                    datetime.now(timezone.utc) - datetime.fromisoformat(ref)
                ).total_seconds()
            except ValueError:
                elapsed = interval
            if elapsed < interval:
                return
        # 先盖章再探测：失败也计入本轮，防止探测风暴
        halt["last_probe_at"] = datetime.now(timezone.utc).isoformat()
        with psycopg.connect(self._conninfo) as conn:
            save_halt(conn, halt)
        logger.info("登录异常熔断已满 %.1f 小时，探测一次登录…", hours)
        try:
            self._provider.force_relogin()
        except BlacklistError as e:
            if getattr(e, "kind", "blacklist") == "blacklist":
                halt.update({"reason": str(e), "kind": "blacklist"})
                with psycopg.connect(self._conninfo) as conn:
                    save_halt(conn, halt)
                logger.error("探测发现 IP 已被拉黑，转为人工处理：%s", e)
            else:
                logger.warning("熔断探测仍失败，下个周期再试：%s", e)
            return
        except Exception as e:
            logger.warning("熔断探测登录失败，下个周期再试：%s", e)
            return
        if getattr(self._provider, "reset_circuit", None):
            self._provider.reset_circuit()
        clear_halt(self._conninfo)
        logger.info("熔断探测登录成功，已自动清除熔断")
        with psycopg.connect(self._conninfo) as conn:
            row = conn.execute(
                "SELECT params, status FROM sync_run ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if row and row[1] == "halted":
            p = row[0]
            ok, msg = self.start(RunParams(
                codes=p.get("codes", []),
                datasets=p.get("datasets", []),
                watchlist_only=p.get("watchlist_only", False),
            ))
            logger.info("自动续跑被熔断中断的任务：%s", msg)

    def _maybe_log_lag(self) -> None:
        """每小时体检一次关注列表日K水位滞后，>2 个交易日则 log 告警（哑火探测）。"""
        now = time.monotonic()
        if now < getattr(self, "_next_lag_check", 0):
            return
        self._next_lag_check = now + 3600
        try:
            import psycopg

            with psycopg.connect(self._conninfo) as conn:
                settled = conn.execute(
                    "SELECT max(calendar_date) FROM trade_calendar "
                    "WHERE is_trading_day AND calendar_date < CURRENT_DATE"
                ).fetchone()[0]
                if settled is None:
                    return
                row = conn.execute(
                    """
                    SELECT w.code,
                           (SELECT count(*) FROM trade_calendar tc
                            WHERE tc.is_trading_day
                              AND tc.calendar_date > COALESCE(m.last_date, '1990-01-01')
                              AND tc.calendar_date <= %s) AS lag
                    FROM watchlist w
                    LEFT JOIN sync_watermark m ON m.code = w.code AND m.dataset = 'k_d'
                    ORDER BY lag DESC LIMIT 1
                    """,
                    (settled,),
                ).fetchone()
            if row is not None and row[1] > 2:
                logger.warning(
                    "数据滞后告警：%s 的日K已落后 %d 个交易日（同步管道可能哑火）",
                    row[0], row[1],
                )
        except Exception:
            logger.exception("滞后体检失败")

    def _maybe_idle_logout(self) -> None:
        try:
            if getattr(self._provider, "should_idle_logout", None) and \
                    self._provider.should_idle_logout():
                self._provider.idle_logout()
        except Exception:
            logger.exception("空闲登出失败")
