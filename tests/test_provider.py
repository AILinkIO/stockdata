"""BaostockProvider 单测：错误分类、relogin 重试一次、10002007 熔断、字段表。零网络。"""

import pytest

import stockdata.provider.baostock as bp
from stockdata.config import Settings
from stockdata.provider.baostock import BaostockProvider, _default_k_fields
from stockdata.provider.interface import (
    BlacklistError,
    DataSourceError,
    LoginError,
    NoDataFoundError,
)
from stockdata.provider.session_guard import MemorySessionStore, SessionGuard

from .fake_bs import FakeBs, FakeResult


def make_settings(**overrides) -> Settings:
    defaults = dict(
        _env_file=None,
        rate_limit_per_minute=0,        # 测试关闭限流
        min_login_interval_seconds=0,   # 测试关闭登录间隔
        watchdog_timeout_seconds=30,
        receive_error_halt_threshold=3,
    )
    defaults.update(overrides)
    return Settings(**defaults)


@pytest.fixture
def fake(monkeypatch):
    fake = FakeBs()
    monkeypatch.setattr(bp, "bs", fake)
    return fake


def make_provider(settings=None) -> BaostockProvider:
    settings = settings or make_settings()
    guard = SessionGuard(MemorySessionStore(), settings.min_login_interval_seconds)
    return BaostockProvider(settings, guard)


def test_lazy_login_and_query(fake):
    p = make_provider()
    fake.script("query_trade_dates", FakeResult(fields=["calendar_date", "is_trading_day"],
                                                rows=[["2026-01-05", "1"]]))
    df = p.query_trade_dates("2026-01-01", "2026-01-10")
    assert fake.login_count == 1
    assert len(df) == 1
    # 第二次查询复用连接，不再登录
    fake.script("query_trade_dates", FakeResult(fields=["calendar_date"], rows=[["2026-01-06"]]))
    p.query_trade_dates("2026-01-06", "2026-01-06")
    assert fake.login_count == 1


def test_no_record_found_maps_to_nodata(fake):
    p = make_provider()
    fake.script("query_dividend_data", FakeResult(error_code="10004", error_msg="no record found"))
    with pytest.raises(NoDataFoundError):
        p.query_dividend("sh.600000", "2020", "operate")


def test_empty_rows_maps_to_nodata(fake):
    p = make_provider()
    fake.script("query_adjust_factor", FakeResult(fields=["code"], rows=[]))
    with pytest.raises(NoDataFoundError):
        p.query_adjust_factor("sh.600000", "2020-01-01", "2020-12-31")


def test_blacklist_error_is_fatal_no_retry(fake):
    p = make_provider()
    fake.script("query_all_stock", FakeResult(error_code="10001011", error_msg="黑名单用户"))
    with pytest.raises(BlacklistError):
        p.query_all_stock("2026-01-05")
    assert fake.login_count == 1  # 未 relogin 重试


def test_retryable_error_relogins_and_retries_once(fake):
    p = make_provider()
    fake.script(
        "query_history_k_data_plus",
        FakeResult(error_code="10002004", error_msg="连接断开"),
        FakeResult(fields=["date", "close"], rows=[["2026-01-05", "10.0"]]),
    )
    df = p.query_k_data("sh.600000", "2026-01-01", "2026-01-05", "d")
    assert len(df) == 1
    assert fake.login_count == 2  # 初次登录 + relogin


def test_retry_exhausted_raises_datasource_error(fake):
    p = make_provider()
    fake.script(
        "query_history_k_data_plus",
        FakeResult(error_code="10002004", error_msg="连接断开"),
        FakeResult(error_code="10002001", error_msg="网络错误"),
    )
    with pytest.raises(DataSourceError):
        p.query_k_data("sh.600000", "2026-01-01", "2026-01-05", "d")


def test_receive_error_fuse_escalates_to_blacklist(fake):
    settings = make_settings(receive_error_halt_threshold=2)
    p = make_provider(settings)
    # 每次查询里 10002007 先按可重试处理（relogin 后重试一次也 10002007）
    # 第 1 次调用：错误计数 1（DataSourceError 可重试）→ relogin 重试 → 计数 2 达阈值 → Blacklist
    fake.script(
        "query_history_k_data_plus",
        FakeResult(error_code="10002007", error_msg="网络接收错误"),
        FakeResult(error_code="10002007", error_msg="网络接收错误"),
    )
    with pytest.raises(BlacklistError):
        p.query_k_data("sh.600000", "2026-01-01", "2026-01-05", "d")


def test_success_resets_receive_error_counter(fake):
    settings = make_settings(receive_error_halt_threshold=2)
    p = make_provider(settings)
    fake.script(
        "query_history_k_data_plus",
        FakeResult(error_code="10002007", error_msg="网络接收错误"),
        FakeResult(fields=["date"], rows=[["2026-01-05"]]),  # relogin 后成功 → 清零
        FakeResult(error_code="10002007", error_msg="网络接收错误"),
        FakeResult(fields=["date"], rows=[["2026-01-06"]]),
    )
    p.query_k_data("sh.600000", "2026-01-01", "2026-01-05", "d")
    p.query_k_data("sh.600000", "2026-01-06", "2026-01-06", "d")  # 计数已清零，不熔断


def test_login_failure_raises_login_error(fake):
    p = make_provider()
    fake.login_result = FakeResult(error_code="10001002", error_msg="登录失败")
    with pytest.raises(LoginError):
        p.query_all_stock("2026-01-05")


def test_login_blacklist_raises_blacklist(fake):
    p = make_provider()
    fake.login_result = FakeResult(error_code="10001011", error_msg="黑名单用户")
    with pytest.raises(BlacklistError):
        p.query_all_stock("2026-01-05")


def test_k_fields_per_frequency():
    assert "peTTM" in _default_k_fields("d")
    assert "preclose" not in _default_k_fields("w")
    assert "time" in _default_k_fields("5")
    assert "time" in _default_k_fields("30")
    for bad in ("15", "60", "m"):
        with pytest.raises(ValueError):
            _default_k_fields(bad)


def test_kline_always_unadjusted(fake):
    p = make_provider()
    fake.script("query_history_k_data_plus", FakeResult(fields=["date"], rows=[["2026-01-05"]]))
    p.query_k_data("sh.600000", "2026-01-01", "2026-01-05", "d")
    _, kwargs = fake.calls[-1]
    assert kwargs["adjustflag"] == "3"


def test_fina_quarter_collects_categories(fake):
    p = make_provider()
    fake.script("query_profit_data", FakeResult(fields=["roeAvg"], rows=[["0.1"]]))
    fake.script("query_operation_data", FakeResult(error_code="10004", error_msg="no record found"))
    # 其余类别用默认结果（1 行）
    result = p.query_fina_quarter("sh.600000", "2025", 3)
    assert result["profit"] == {"roeAvg": "0.1"}
    assert "operation" not in result
    assert set(result) == {"profit", "growth", "balance", "cash_flow", "dupont"}
