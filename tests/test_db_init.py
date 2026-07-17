"""db init/reset 集成测试（stockdata_e2e 库）。"""

import psycopg

from stockdata.db.init import init_schema, list_tables, reset_db

EXPECTED_TABLES = {
    "schema_meta", "security", "watchlist", "trade_calendar",
    "kline", "kline_minute", "adjust_factor", "dividend", "financial_report",
    "stock_industry", "index_constituent", "stock_list_snapshot", "macro_data",
    "sync_watermark", "sync_run", "baostock_session", "sync_state",
}


def test_init_creates_all_tables(pg_db):
    assert set(list_tables(pg_db)) == EXPECTED_TABLES


def test_init_is_idempotent(pg_db):
    init_schema(pg_db)  # 第二次执行不报错
    with psycopg.connect(pg_db) as conn:
        n = conn.execute("SELECT count(*) FROM baostock_session").fetchone()[0]
        assert n == 1  # 单行不重复插入
        v = conn.execute("SELECT max(version) FROM schema_meta").fetchone()[0]
        assert v == 1


def test_reset_drops_everything(pg_db):
    dropped = reset_db(pg_db)
    assert set(dropped) == EXPECTED_TABLES
    assert list_tables(pg_db) == []
    init_schema(pg_db)
    assert set(list_tables(pg_db)) == EXPECTED_TABLES
