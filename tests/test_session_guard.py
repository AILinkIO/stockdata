"""SessionGuard 单测：5 分钟登录间隔红线。"""

from datetime import UTC, datetime, timedelta

from stockdata.provider.session_guard import MemorySessionStore, SessionGuard


def test_first_login_no_wait():
    slept: list[float] = []
    guard = SessionGuard(MemorySessionStore(), 300, sleep=slept.append)
    guard.before_login()
    assert slept == []


def test_login_within_interval_sleeps_remainder():
    store = MemorySessionStore()
    t0 = datetime(2026, 7, 17, 8, 0, 0, tzinfo=UTC)
    store.stamp_login(t0)
    slept: list[float] = []
    guard = SessionGuard(
        store, 300, sleep=slept.append, now=lambda: t0 + timedelta(seconds=100)
    )
    guard.before_login()
    assert slept == [200.0]


def test_login_after_interval_no_wait():
    store = MemorySessionStore()
    t0 = datetime(2026, 7, 17, 8, 0, 0, tzinfo=UTC)
    store.stamp_login(t0)
    slept: list[float] = []
    guard = SessionGuard(
        store, 300, sleep=slept.append, now=lambda: t0 + timedelta(seconds=301)
    )
    guard.before_login()
    assert slept == []


def test_stamp_written_before_login_attempt():
    store = MemorySessionStore()
    now = datetime(2026, 7, 17, 9, 0, 0, tzinfo=UTC)
    guard = SessionGuard(store, 300, sleep=lambda s: None, now=lambda: now)
    guard.before_login()
    assert store.last_login_at() == now  # 失败的 login 尝试也占用间隔


def test_zero_interval_disables_guard():
    store = MemorySessionStore()
    store.stamp_login(datetime.now(UTC))
    slept: list[float] = []
    guard = SessionGuard(store, 0, sleep=slept.append)
    guard.before_login()
    assert slept == []
