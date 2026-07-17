"""同步水位（sync_watermark）：每 (code, dataset) 的覆盖区间 = 持久断点。

语义（移植自旧 dotnet Coverage）：
- [first_date, last_date] 为连续闭区间覆盖；last_date 只推进到**实际拿到数据的最后日**，
  或空结果时推进到切片的「已结算边界」（记「查过、无数据」）——未结算尾部绝不虚报。
- last_synced_at 为系统水位：上次成功同步完成时刻，用于 stale 判定。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import psycopg


@dataclass
class Watermark:
    code: str
    dataset: str
    first_date: date | None
    last_date: date | None
    last_synced_at: datetime | None


def get(conn: psycopg.Connection, code: str, dataset: str) -> Watermark | None:
    row = conn.execute(
        "SELECT first_date, last_date, last_synced_at FROM sync_watermark "
        "WHERE code = %s AND dataset = %s",
        (code, dataset),
    ).fetchone()
    if row is None:
        return None
    return Watermark(code, dataset, row[0], row[1], row[2])


def advance(
    conn: psycopg.Connection,
    code: str,
    dataset: str,
    first: date | None,
    last: date | None,
) -> None:
    """推进覆盖区间（LEAST/GREATEST 幂等）并盖 last_synced_at。first/last 可为 None（只盖时间）。"""
    conn.execute(
        """
        INSERT INTO sync_watermark (code, dataset, first_date, last_date, last_synced_at)
        VALUES (%s,%s,%s,%s,now())
        ON CONFLICT (code, dataset) DO UPDATE SET
            first_date = LEAST(COALESCE(sync_watermark.first_date, EXCLUDED.first_date),
                               COALESCE(EXCLUDED.first_date, sync_watermark.first_date)),
            last_date = GREATEST(COALESCE(sync_watermark.last_date, EXCLUDED.last_date),
                                 COALESCE(EXCLUDED.last_date, sync_watermark.last_date)),
            last_synced_at = now()
        """,
        (code, dataset, first, last),
    )


def all_for_code(conn: psycopg.Connection, code: str) -> list[Watermark]:
    rows = conn.execute(
        "SELECT dataset, first_date, last_date, last_synced_at FROM sync_watermark "
        "WHERE code = %s ORDER BY dataset",
        (code,),
    ).fetchall()
    return [Watermark(code, r[0], r[1], r[2], r[3]) for r in rows]
