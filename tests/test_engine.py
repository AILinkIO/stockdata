"""同步引擎集成测试（PG stockdata_e2e + FakeProvider）。"""

import threading
from datetime import date

import psycopg
import pytest

from stockdata.config import Settings
from stockdata.provider.interface import BlacklistError, DataSourceError
from stockdata.sync.engine import (
    AnotherRunActive,
    EngineEvents,
    HaltError,
    RunParams,
    SyncEngine,
    clear_halt,
)

from .fake_provider import FakeProvider

TODAY = date(2026, 7, 17)  # 周五
SETTLED = date(2026, 7, 16)


def make_settings(**overrides) -> Settings:
    defaults = dict(
        _env_file=None,
        rate_limit_per_minute=0,
        min_login_interval_seconds=0,
        max_retries=1,
        retry_base_seconds=0,
        retry_max_backoff_seconds=0,
        minute_backfill_floor=date(2026, 6, 1),
        financial_backfill_floor=date(2026, 1, 1),
        tail_refresh_days=0,  # 大多数用例要"零调用增量"语义；尾部修正单独测
    )
    defaults.update(overrides)
    return Settings(**defaults)


def make_engine(dsn, provider, settings=None, stop_event=None, events=None) -> SyncEngine:
    return SyncEngine(dsn, provider, settings or make_settings(),
                      events=events, stop_event=stop_event)


def q1(dsn, sql, *args):
    with psycopg.connect(dsn) as conn:
        return conn.execute(sql, args).fetchone()


def test_full_run_writes_everything(pg_db):
    fake = FakeProvider()
    stats = make_engine(pg_db, fake).run(RunParams(), today=TODAY)

    assert stats.status == "done"
    assert stats.total_codes == 2 and stats.done_codes == 2
    assert not stats.errors

    # 市场级
    assert q1(pg_db, "SELECT count(*) FROM trade_calendar")[0] > 9000
    assert q1(pg_db, "SELECT count(*) FROM security")[0] == 2
    assert q1(pg_db, "SELECT count(*) FROM stock_list_snapshot")[0] == 2
    assert q1(pg_db, "SELECT count(*) FROM stock_industry")[0] == 2
    assert q1(pg_db, "SELECT count(*) FROM index_constituent")[0] == 3  # 3 指数各 1 行
    assert q1(pg_db, "SELECT count(DISTINCT kind) FROM macro_data")[0] == 5

    # 按码
    for code in ("sh.600000", "sz.000001"):
        for freq in ("d", "w"):
            n = q1(pg_db, "SELECT count(*) FROM kline WHERE code=%s AND frequency=%s",
                   code, freq)[0]
            assert n > 0, (code, freq)
        for freq in ("5", "30"):
            n = q1(pg_db, "SELECT count(*) FROM kline_minute WHERE code=%s AND frequency=%s",
                   code, freq)[0]
            assert n > 0, (code, freq)
    assert q1(pg_db, "SELECT count(*) FROM adjust_factor")[0] == 1
    assert q1(pg_db, "SELECT count(*) FROM dividend")[0] == 1
    assert q1(pg_db, "SELECT count(*) FROM financial_report WHERE report_type='profit'")[0] == 2

    # 水位：k_d 推进到结算日（16 日，17 日盘中不结算）
    wm = q1(pg_db, "SELECT last_date FROM sync_watermark WHERE code='sh.600000' AND dataset='k_d'")
    assert wm[0] == SETTLED
    # 空结果（业绩快报）也在结算区内推进（today-7）
    wm = q1(pg_db, "SELECT last_date FROM sync_watermark "
                   "WHERE code='sh.600000' AND dataset='performance_express'")
    assert wm[0] == TODAY.replace(day=10)

    # run 历史落库
    row = q1(pg_db, "SELECT status, stats->>'done_codes' FROM sync_run ORDER BY id DESC LIMIT 1")
    assert row == ("done", "2")


def test_second_run_is_fully_incremental(pg_db):
    fake = FakeProvider()
    engine = make_engine(pg_db, fake)
    engine.run(RunParams(), today=TODAY)

    fake.calls.clear()
    stats = make_engine(pg_db, fake).run(RunParams(), today=TODAY)
    assert stats.status == "done"
    # 全部数据集已覆盖/fresh：零 baostock 调用
    assert fake.calls == []


def test_tail_refresh_repulls_recent_window(pg_db):
    """尾部修正：已覆盖时日/周线仍重拉最近 N 天（单片单调用），分钟线不受影响。"""
    from datetime import timedelta

    fake = FakeProvider()
    settings = make_settings(tail_refresh_days=5)
    make_engine(pg_db, fake, settings).run(RunParams(codes=["sh.600000"]), today=TODAY)

    fake.calls.clear()
    stats = make_engine(pg_db, fake, settings).run(
        RunParams(codes=["sh.600000"]), today=TODAY
    )
    assert stats.status == "done" and not stats.errors

    d_calls = fake.calls_of("query_k_data", frequency="d")
    assert len(d_calls) == 1
    assert d_calls[0]["start_date"] == (SETTLED - timedelta(days=4)).isoformat()
    assert d_calls[0]["end_date"] == SETTLED.isoformat()
    assert len(fake.calls_of("query_k_data", frequency="w")) == 1
    assert fake.calls_of("query_k_data", frequency="5") == []  # 分钟线零调用


def test_index_code_syncs_daily_but_skips_minute(pg_db):
    """指数（type=2）：日/周线正常同步，分钟线阶段跳过（baostock 不支持指数分钟线）。"""
    fake = FakeProvider(codes=("sh.600000", "sh.000001"), index_codes=("sh.000001",))
    make_engine(pg_db, fake).run(RunParams(), today=TODAY)  # 全量建 security（含 type）

    fake.calls.clear()
    with psycopg.connect(pg_db) as conn:
        conn.execute("DELETE FROM sync_watermark WHERE dataset IN ('k_5', 'k_30')")
    stats = make_engine(pg_db, fake).run(
        RunParams(codes=["sh.600000", "sh.000001"]), today=TODAY
    )
    assert stats.status == "done"
    minute_codes = {c["code"] for c in fake.calls_of("query_k_data", frequency="5")}
    assert minute_codes == {"sh.600000"}  # 指数被跳过
    # 指数日线已入库
    assert q1(pg_db, "SELECT count(*) FROM kline WHERE code='sh.000001' "
                     "AND frequency='d'")[0] > 0


def test_resume_after_dataset_failure(pg_db):
    fake = FakeProvider()

    def fail_weekly(method, kwargs):
        if method == "query_k_data" and kwargs["frequency"] == "w":
            raise DataSourceError("模拟周线故障")

    fake.hooks.append(fail_weekly)
    stats = make_engine(pg_db, fake).run(RunParams(codes=["sh.600000"]), today=TODAY)
    assert stats.status == "done"  # 单数据集失败不终止 run
    assert any(e["dataset"] == "k_w" for e in stats.errors)
    assert q1(pg_db, "SELECT count(*) FROM kline WHERE frequency='w'")[0] == 0
    assert q1(pg_db, "SELECT count(*) FROM kline WHERE frequency='d'")[0] > 0

    # 重跑（故障移除）：k_d 已覆盖不重抓，k_w 从头补
    fake.hooks.clear()
    fake.calls.clear()
    stats2 = make_engine(pg_db, fake).run(RunParams(codes=["sh.600000"]), today=TODAY)
    assert stats2.status == "done" and not stats2.errors
    assert fake.calls_of("query_k_data", frequency="d") == []
    assert len(fake.calls_of("query_k_data", frequency="w")) == 1
    assert q1(pg_db, "SELECT count(*) FROM kline WHERE frequency='w'")[0] > 0


def test_blacklist_halts_and_persists(pg_db):
    fake = FakeProvider()

    def blacklist_on_kline(method, kwargs):
        if method == "query_k_data":
            raise BlacklistError("模拟 10001011 拉黑")

    fake.hooks.append(blacklist_on_kline)
    stats = make_engine(pg_db, fake).run(RunParams(codes=["sh.600000"]), today=TODAY)
    assert stats.status == "halted"
    assert q1(pg_db, "SELECT value->>'reason' FROM sync_state WHERE key='halt'") is not None

    # halt 持久：新引擎实例拒绝启动
    with pytest.raises(HaltError):
        make_engine(pg_db, FakeProvider()).run(RunParams(codes=["sh.600000"]), today=TODAY)

    # clear-halt 后恢复
    assert clear_halt(pg_db)
    stats2 = make_engine(pg_db, FakeProvider()).run(RunParams(codes=["sh.600000"]), today=TODAY)
    assert stats2.status == "done"


def test_retry_backoff_then_success(pg_db):
    fake = FakeProvider()
    failures = {"n": 0}

    def flaky(method, kwargs):
        if method == "query_trade_dates" and failures["n"] < 1:
            failures["n"] += 1
            raise DataSourceError("瞬时网络错误")

    fake.hooks.append(flaky)
    stats = make_engine(pg_db, fake).run(RunParams(codes=["sh.600000"]), today=TODAY)
    assert stats.status == "done"
    assert not stats.errors
    assert len(fake.calls_of("query_trade_dates")) == 2  # 失败一次 + 重试成功


def test_stop_event_stops_cleanly(pg_db):
    fake = FakeProvider()
    stop = threading.Event()

    class StopAfterFirstSlice(EngineEvents):
        def slice_done(self, code, dataset, label, rows):
            stop.set()

    stats = make_engine(pg_db, fake, stop_event=stop,
                        events=StopAfterFirstSlice()).run(RunParams(), today=TODAY)
    assert stats.status == "stopped"
    # 第一个切片（交易日历）已提交，水位在
    assert q1(pg_db, "SELECT count(*) FROM trade_calendar")[0] > 0


def test_advisory_lock_blocks_second_instance(pg_db):
    # 用独立连接持锁模拟并发实例
    holder = psycopg.connect(pg_db)
    holder.execute("SELECT pg_advisory_lock(hashtext('stockdata_sync'))")
    holder.commit()
    try:
        with pytest.raises(AnotherRunActive):
            make_engine(pg_db, FakeProvider()).run(RunParams(), today=TODAY)
    finally:
        holder.close()


def test_dataset_filter(pg_db):
    fake = FakeProvider()
    stats = make_engine(pg_db, fake).run(
        RunParams(codes=["sh.600000"], datasets=["k_d"]), today=TODAY
    )
    assert stats.status == "done"
    assert q1(pg_db, "SELECT count(*) FROM kline WHERE frequency='d'")[0] > 0
    assert q1(pg_db, "SELECT count(*) FROM kline WHERE frequency='w'")[0] == 0
    assert q1(pg_db, "SELECT count(*) FROM dividend")[0] == 0
