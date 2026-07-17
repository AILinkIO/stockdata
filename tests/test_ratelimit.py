"""MemoryRateLimiter 滑动窗口单测。"""

from stockdata.core.ratelimit import MemoryRateLimiter


def test_burst_within_limit():
    rl = MemoryRateLimiter(max_calls=3, period=60)
    assert rl.try_acquire()
    assert rl.try_acquire()
    assert rl.try_acquire()
    assert not rl.try_acquire()
    assert rl.current_rate() == 3


def test_disabled_when_nonpositive():
    rl = MemoryRateLimiter(max_calls=0)
    for _ in range(100):
        assert rl.try_acquire()


def test_window_slides(monkeypatch):
    import stockdata.core.ratelimit as m

    t = [0.0]
    monkeypatch.setattr(m.time, "monotonic", lambda: t[0])
    rl = MemoryRateLimiter(max_calls=2, period=60)
    assert rl.try_acquire()
    assert rl.try_acquire()
    assert not rl.try_acquire()
    t[0] = 61.0  # 最早的调用滑出窗口
    assert rl.try_acquire()
