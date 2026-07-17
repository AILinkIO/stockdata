"""熔断自动探测：仅 login_error 类每 N 小时探测，成功清除并续跑；拉黑绝不探测。"""

import json
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from stockdata.config import Settings
from stockdata.sync.engine import read_halt
from stockdata.sync.runner import SyncRunner

from .fake_provider import FakeProvider


def _save_halt(dsn: str, kind: str, hours_ago: float, probe_hours_ago=None) -> None:
    value = {
        "reason": "网络接收错误",
        "kind": kind,
        "halted_at": (
            datetime.now(timezone.utc) - timedelta(hours=hours_ago)
        ).isoformat(),
    }
    if probe_hours_ago is not None:
        value["last_probe_at"] = (
            datetime.now(timezone.utc) - timedelta(hours=probe_hours_ago)
        ).isoformat()
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO sync_state (key, value) VALUES ('halt', %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
            (json.dumps(value),),
        )


class _ProbeProvider(FakeProvider):
    """记录 force_relogin 调用；可注入失败。"""

    def __init__(self, fail: Exception | None = None):
        super().__init__()
        self.relogin_calls = 0
        self.fail = fail
        self.circuit_reset = False

    def force_relogin(self):
        self.relogin_calls += 1
        if self.fail is not None:
            raise self.fail

    def reset_circuit(self):
        self.circuit_reset = True


@pytest.fixture
def runner_env(pg_db):
    settings = Settings(pg_dsn=pg_db, halt_probe_interval_hours=4)
    provider = _ProbeProvider()
    runner = SyncRunner(settings.pg_conninfo, provider, settings)
    yield pg_db, runner, provider
    runner.shutdown()


def _halt(dsn: str) -> dict | None:
    with psycopg.connect(dsn) as conn:
        return read_halt(conn)


def test_login_error_probe_success_clears_and_resumes(runner_env):
    dsn, runner, provider = runner_env
    _save_halt(dsn, "login_error", hours_ago=5)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO sync_run (params, status, finished_at) "
            "VALUES (%s, 'halted', now())",
            (json.dumps({"codes": ["sh.600000"], "datasets": ["k_d"]}),),
        )

    runner._probe_halt(4)

    assert provider.relogin_calls == 1
    assert provider.circuit_reset is True
    assert _halt(dsn) is None                     # 熔断已清除
    assert runner._pending is not None or runner.state()["running"]  # 已请求续跑


def test_login_error_probe_too_early(runner_env):
    dsn, runner, provider = runner_env
    _save_halt(dsn, "login_error", hours_ago=2)   # 未满 4 小时
    runner._probe_halt(4)
    assert provider.relogin_calls == 0
    assert _halt(dsn) is not None


def test_probe_interval_measured_from_last_probe(runner_env):
    dsn, runner, provider = runner_env
    _save_halt(dsn, "login_error", hours_ago=10, probe_hours_ago=1)  # 1 小时前刚探测过
    runner._probe_halt(4)
    assert provider.relogin_calls == 0


def test_blacklist_never_probed(runner_env):
    dsn, runner, provider = runner_env
    _save_halt(dsn, "blacklist", hours_ago=100)
    runner._probe_halt(4)
    assert provider.relogin_calls == 0
    assert _halt(dsn)["kind"] == "blacklist"


def test_probe_failure_keeps_halt_and_stamps(runner_env):
    dsn, runner, provider = runner_env
    provider.fail = ConnectionError("still down")
    _save_halt(dsn, "login_error", hours_ago=5)

    runner._probe_halt(4)

    assert provider.relogin_calls == 1
    halt = _halt(dsn)
    assert halt is not None and halt["kind"] == "login_error"
    assert "last_probe_at" in halt                # 已盖章，防探测风暴

    # 盖章后立即再探测 → 不动
    runner._probe_halt(4)
    assert provider.relogin_calls == 1


def test_probe_discovers_blacklist_escalates(runner_env):
    from stockdata.provider.interface import BlacklistError

    dsn, runner, provider = runner_env
    provider.fail = BlacklistError("黑名单用户 (code: 10001011)", kind="blacklist")
    _save_halt(dsn, "login_error", hours_ago=5)

    runner._probe_halt(4)

    halt = _halt(dsn)
    assert halt["kind"] == "blacklist"            # 升级为拉黑，之后不再探测
    runner._probe_halt(4)
    assert provider.relogin_calls == 1
