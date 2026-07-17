"""/api/v1 数据面集成测试：FakeProvider 全量同步一次种子数据，随后纯读。"""

import time

import pytest
from fastapi.testclient import TestClient

from .fake_provider import FakeProvider


def _wait_finished(client: TestClient, timeout: float = 120) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = client.get("/api/sync/status").json()["state"]
        if st["status"] and not st["running"]:
            assert st["status"] == "done", st
            return
        time.sleep(0.2)
    raise AssertionError("种子同步未在期限内结束")


@pytest.fixture(scope="module")
def api(pg_dsn):
    """模块级共享：重置库 → FakeProvider 全量同步种子 → 只读测试复用。"""
    from stockdata.db.init import init_schema, reset_db

    reset_db(pg_dsn)
    init_schema(pg_dsn)

    from stockdata.config import settings as cfg

    old_dsn = cfg.pg_dsn
    cfg.pg_dsn = pg_dsn
    import stockdata.db.pool as pool_mod

    pool_mod._pool = None

    import stockdata.web.app as webapp
    from nicegui import app as fastapi_app

    webapp.init_runner(provider=FakeProvider())
    client = TestClient(fastapi_app)
    assert client.post("/api/sync/run", json={}).status_code == 202
    _wait_finished(client)
    yield client
    webapp.shutdown_runner()
    cfg.pg_dsn = old_dsn
    pool_mod._pool = None


# ── 行情 ──


def test_kline_daily(api):
    r = api.get("/api/v1/kline/sh.600000", params={"freq": "d"}).json()
    assert r["meta"]["count"] > 200
    row = r["data"][0]
    assert {"trade_date", "open", "close", "volume", "pe_ttm"} <= set(row)
    assert float(row["close"]) == 10.5


def test_kline_adjust_fore_back(api):
    # FakeProvider：sh.600000 在 2025-06-10 有 back 因子 1.25
    back = api.get("/api/v1/kline/sh.600000",
                   params={"freq": "d", "adjust": "back"}).json()["data"]
    assert float(back[0]["close"]) == 10.5          # 除权日前 B=1
    assert float(back[-1]["close"]) == 13.125       # 之后 ×1.25
    fore = api.get("/api/v1/kline/sh.600000",
                   params={"freq": "d", "adjust": "fore"}).json()["data"]
    assert float(fore[0]["close"]) == 8.4           # 之前 ÷1.25
    assert float(fore[-1]["close"]) == 10.5


def test_kline_range_and_truncate(api):
    r = api.get("/api/v1/kline/sh.600000", params={
        "freq": "d", "start": "2026-01-01", "end": "2026-01-31", "limit": 5,
    }).json()
    assert r["meta"]["count"] == 5 and r["meta"]["truncated"] is True
    assert r["data"][0]["trade_date"] >= "2026-01-01"


def test_kline_minute(api):
    r = api.get("/api/v1/kline/sz.000001", params={"freq": "5"}).json()
    assert r["meta"]["count"] > 0
    assert "bar_time" in r["data"][0]


def test_kline_batch(api):
    r = api.post("/api/v1/kline/batch", json={
        "codes": ["sh.600000", "sz.000001"], "freq": "w", "adjust": "back",
    }).json()
    assert set(r["data"]) == {"sh.600000", "sz.000001"}
    assert all(rows for rows in r["data"].values())


def test_batch_codes_cap(api):
    resp = api.post("/api/v1/kline/batch",
                    json={"codes": [f"sh.{i:06d}" for i in range(501)]})
    assert resp.status_code == 422


def test_adjust_factors(api):
    r = api.get("/api/v1/adjust-factors/sh.600000").json()
    assert r["meta"]["count"] == 1
    assert float(r["data"][0]["back_adjust_factor"]) == 1.25


# ── 参考数据 ──


def test_securities_list_and_detail(api):
    r = api.get("/api/v1/securities", params={"q": "600000"}).json()
    assert r["meta"]["total"] == 1 and r["data"][0]["code"] == "sh.600000"
    detail = api.get("/api/v1/securities/sh.600000").json()["data"]
    assert detail["code_name"] == "股票0" and detail["industry"] == "银行"
    assert api.get("/api/v1/securities/sh.999999").status_code == 404


def test_trade_calendar(api):
    r = api.get("/api/v1/trade-calendar", params={
        "start": "2026-07-06", "end": "2026-07-12", "only_trading": True,
    }).json()
    assert r["meta"]["count"] == 5  # 工作日为交易日


def test_industries_and_index(api):
    r = api.get("/api/v1/industries").json()
    assert r["meta"]["snap_date"] and r["meta"]["count"] == 2
    r = api.get("/api/v1/index-constituents/sz50").json()
    assert r["meta"]["count"] == 1 and r["data"][0]["code"] == "sh.600000"
    assert api.get("/api/v1/index-constituents/nope").status_code == 422


# ── 财务 / 事件 ──


def test_financials(api):
    r = api.get("/api/v1/financials/sh.600000", params={"type": "profit"}).json()
    assert r["meta"]["count"] == 1
    assert r["data"][0]["stat_date"] == "2026-03-31"
    batch = api.post("/api/v1/financials/batch", json={
        "codes": ["sh.600000", "sz.000001"], "type": "profit",
    }).json()
    assert batch["meta"]["count"] >= 1


def test_dividends(api):
    r = api.get("/api/v1/dividends/sh.600000", params={"year": 2025}).json()
    assert r["meta"]["count"] == 1
    assert r["data"][0]["operate_date"] == "2025-06-10"
    none = api.get("/api/v1/dividends/sh.600000", params={"year": 2020}).json()
    assert none["meta"]["count"] == 0


# ── 宏观 / 元数据 ──


def test_macro(api):
    r = api.get("/api/v1/macro/deposit_rate").json()
    assert r["meta"]["count"] == 1 and r["data"][0]["date_key"] == "2025-10-01"
    assert api.get("/api/v1/macro/nope").status_code == 422


def test_watermarks(api):
    r = api.get("/api/v1/meta/watermarks", params={"dataset": "k_d"}).json()
    assert r["meta"]["total"] == 2
    assert {w["code"] for w in r["data"]} == {"sh.600000", "sz.000001"}


# ── 鉴权 ──


def test_api_key_switch(api):
    from stockdata.config import settings as cfg

    cfg.api_key = "sekrit"
    try:
        assert api.get("/api/v1/securities").status_code == 401
        ok = api.get("/api/v1/securities", headers={"X-API-Key": "sekrit"})
        assert ok.status_code == 200
        # 内部控制面 /api/sync/* 不受数据面 Key 影响
        assert api.get("/api/sync/status").status_code == 200
    finally:
        cfg.api_key = ""
