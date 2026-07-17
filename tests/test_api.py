"""REST API 集成测试：FakeProvider 注入 SyncRunner，TestClient 走完整 HTTP 面。"""

import time

import pytest
from fastapi.testclient import TestClient

from .fake_provider import FakeProvider


@pytest.fixture
def api(pg_db, monkeypatch):
    from stockdata.config import settings as cfg

    monkeypatch.setattr(cfg, "pg_dsn", pg_db)

    # db.pool 的进程级单例可能指向上一个测试库，强制重建
    import stockdata.db.pool as pool_mod

    monkeypatch.setattr(pool_mod, "_pool", None)

    import stockdata.web.app as webapp
    from nicegui import app as fastapi_app

    webapp.init_runner(provider=FakeProvider())
    yield TestClient(fastapi_app)
    webapp.shutdown_runner()


def _wait_finished(client: TestClient, timeout: float = 60) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = client.get("/api/sync/status").json()
        st = data["state"]
        if st["status"] and not st["running"]:
            return data
        time.sleep(0.2)
    raise AssertionError("同步未在期限内结束")


def test_healthz(api):
    assert api.get("/healthz").json()["status"] == "ok"


def test_run_lifecycle_over_http(api):
    resp = api.post("/api/sync/run", json={"codes": ["sh.600000"]})
    assert resp.status_code == 202

    data = _wait_finished(api)
    st = data["state"]
    assert st["status"] == "done"
    assert st["rows_total"] > 0
    assert data["halt"] is None

    overview = api.get("/api/sync/overview").json()
    assert overview["runs"][0]["status"] == "done"
    datasets = {d["dataset"] for d in overview["watermarks"]["datasets"]}
    assert "k_d" in datasets and "k_5" in datasets


def test_stop_and_clear_halt_endpoints(api):
    # 空闲时 stop 返回 stopping=false
    assert api.post("/api/sync/stop").json() == {"stopping": False}
    # 无熔断时 clear-halt 返回 cleared=false
    assert api.post("/api/sync/clear-halt").json() == {"cleared": False}
