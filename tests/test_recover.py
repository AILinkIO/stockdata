"""崩溃恢复：孤儿 running 收尾 + 最新 interrupted 才自动续跑。"""

import json

import psycopg

from stockdata.sync.engine import ADVISORY_LOCK_KEY, recover_interrupted_run


def _insert(conn, status: str, params: dict) -> int:
    return conn.execute(
        "INSERT INTO sync_run (params, status, finished_at) VALUES "
        "(%s, %s, CASE WHEN %s = 'running' THEN NULL ELSE now() END) RETURNING id",
        (json.dumps(params), status, status),
    ).fetchone()[0]


def _status(conn, run_id: int) -> str:
    return conn.execute(
        "SELECT status FROM sync_run WHERE id = %s", (run_id,)
    ).fetchone()[0]


def test_crash_orphan_marked_and_resumed(pg_db):
    with psycopg.connect(pg_db, autocommit=True) as conn:
        _insert(conn, "done", {"codes": []})
        orphan = _insert(conn, "running", {"codes": ["sh.600050"], "datasets": ["k_d"]})

        params = recover_interrupted_run(pg_db)

        assert params == {"codes": ["sh.600050"], "datasets": ["k_d"]}
        assert _status(conn, orphan) == "interrupted"
        assert conn.execute(
            "SELECT finished_at FROM sync_run WHERE id = %s", (orphan,)
        ).fetchone()[0] is not None


def test_old_orphan_marked_but_not_resumed(pg_db):
    with psycopg.connect(pg_db, autocommit=True) as conn:
        orphan = _insert(conn, "running", {"codes": ["sh.600050"]})
        _insert(conn, "done", {"codes": []})

        assert recover_interrupted_run(pg_db) is None
        assert _status(conn, orphan) == "interrupted"  # 收尾但不续跑


def test_user_stopped_or_halted_not_resumed(pg_db):
    with psycopg.connect(pg_db, autocommit=True) as conn:
        _insert(conn, "stopped", {"codes": ["sh.600050"]})
        assert recover_interrupted_run(pg_db) is None
        _insert(conn, "halted", {"codes": ["sh.600050"]})
        assert recover_interrupted_run(pg_db) is None


def test_shutdown_interrupted_is_resumed(pg_db):
    """进程关停打断（runner 标记 interrupted）→ 下次启动续跑。"""
    with psycopg.connect(pg_db, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO sync_run (params, status, finished_at) "
            "VALUES (%s, 'interrupted', now())",
            (json.dumps({"watchlist_only": True}),),
        )
        assert recover_interrupted_run(pg_db) == {"watchlist_only": True}


def test_active_instance_lock_guard(pg_db):
    """advisory lock 被占（另一实例真在跑）→ 不动任何行、不续跑。"""
    with psycopg.connect(pg_db, autocommit=True) as conn:
        run_id = _insert(conn, "running", {"codes": ["sh.600050"]})

        with psycopg.connect(pg_db, autocommit=True) as holder:
            assert holder.execute(
                "SELECT pg_try_advisory_lock(hashtext(%s))", (ADVISORY_LOCK_KEY,)
            ).fetchone()[0]
            assert recover_interrupted_run(pg_db) is None
            assert _status(conn, run_id) == "running"  # 原样保留

        # 锁释放后可正常收尾
        assert recover_interrupted_run(pg_db) == {"codes": ["sh.600050"]}
