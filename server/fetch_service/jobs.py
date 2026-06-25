"""
Redis job 存储（D-C：完成态 TTL 600s、在途/僵尸阈值 1200s）。

key 布局：
  job:{id}              hash   type/params/params_hash/status/error/created_at/started_at/finished_at
  job:result:{id}       string payload JSON（独立 key 便于大 payload 单独过期）
  job:idx:{params_hash} string → job_id（去重索引，在飞搭车）
  fetch:pending         list   待消费 job_id（worker BRPOP）

去重语义复刻旧 dispatch._params_hash：同 (type, params) 在飞或刚完成（TTL 内）→ 返回同 job_id。
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid

import redis

RESULT_TTL = 600     # 完成态 job + payload + dedup 索引留存（沿用旧 result_expires）
INFLIGHT_TTL = 1200  # 在途 job 安全 TTL / 僵尸阈值（沿用旧 _STALE_RUNNING）

_PENDING_KEY = "fetch:pending"
_HALTED_KEY = "fetch:halted"  # 抓取暂停标志（baostock 拉黑），持久无 TTL，仅 /restart 清除


def _job_redis_url() -> str:
    # 私有 job 存储，默认独立 DB2（限流用 db1），避免相互冲突
    return os.getenv("STOCKDATA_FETCH_JOB_REDIS_URL", "redis://127.0.0.1:6379/2")


def params_hash(task_type: str, params: dict) -> str:
    payload = json.dumps({"task": task_type, **params}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


class JobStore:
    def __init__(self, url: str | None = None) -> None:
        self._r = redis.Redis.from_url(url or _job_redis_url(), decode_responses=True)

    # ── 提交 / 去重 ──

    def submit(self, task_type: str, params: dict) -> tuple[str, str, bool]:
        """返回 (job_id, status, dedup)。同参数在飞/未过期 → 复用既有 job。"""
        h = params_hash(task_type, params)
        idx_key = f"job:idx:{h}"
        new_id = uuid.uuid4().hex

        # SETNX 抢占去重索引；抢到才建新 job
        if self._r.set(idx_key, new_id, nx=True, ex=INFLIGHT_TTL):
            now = int(time.time())
            self._r.hset(f"job:{new_id}", mapping={
                "type": task_type,
                "params": json.dumps(params),
                "params_hash": h,
                "status": "pending",
                "created_at": now,
            })
            self._r.expire(f"job:{new_id}", INFLIGHT_TTL)
            self._r.lpush(_PENDING_KEY, new_id)
            return new_id, "pending", False

        # 已存在：返回既有 job_id 与当前状态（搭车）
        existing = self._r.get(idx_key)
        status = self._r.hget(f"job:{existing}", "status") if existing else None
        return (existing or new_id), (status or "pending"), True

    # ── 抓取暂停标志（baostock 拉黑/接收错误）──

    def set_halted(self, reason: str) -> None:
        """标记抓取已暂停。持久（无 TTL），跨容器重启保留，直到 /restart 清除。"""
        self._r.hset(_HALTED_KEY, mapping={"reason": reason, "since": int(time.time())})

    def clear_halted(self) -> None:
        self._r.delete(_HALTED_KEY)

    def halted_state(self) -> dict | None:
        """返回 {reason, since} 或 None（未暂停）。"""
        h = self._r.hgetall(_HALTED_KEY)
        if not h:
            return None
        return {"reason": h.get("reason", ""), "since": int(h.get("since", 0))}

    # ── worker 侧 ──

    def next_pending(self, timeout: int = 5) -> str | None:
        res = self._r.brpop(_PENDING_KEY, timeout=timeout)
        return res[1] if res else None

    def task_of(self, job_id: str) -> tuple[str, dict] | None:
        """取 job 的 (type, params)，供 worker 执行。job 不存在返回 None。"""
        meta = self._r.hgetall(f"job:{job_id}")
        if not meta:
            return None
        return meta.get("type", ""), json.loads(meta.get("params", "{}"))

    def mark_running(self, job_id: str) -> None:
        self._r.hset(f"job:{job_id}", mapping={"status": "running", "started_at": int(time.time())})
        self._r.expire(f"job:{job_id}", INFLIGHT_TTL)  # 心跳续期，防长回填段误判僵尸

    def mark_done(self, job_id: str, payload: dict) -> None:
        self._r.set(f"job:result:{job_id}", json.dumps(payload), ex=RESULT_TTL)
        self._r.hset(f"job:{job_id}", mapping={"status": "done", "finished_at": int(time.time())})
        self._compress_ttl(job_id)

    def mark_failed(self, job_id: str, error: str) -> None:
        self._r.hset(f"job:{job_id}", mapping={
            "status": "failed", "error": error, "finished_at": int(time.time()),
        })
        self._compress_ttl(job_id)

    def _compress_ttl(self, job_id: str) -> None:
        """完成/失败：把 job 与去重索引 TTL 压到 RESULT_TTL。"""
        self._r.expire(f"job:{job_id}", RESULT_TTL)
        h = self._r.hget(f"job:{job_id}", "params_hash")
        if h:
            self._r.expire(f"job:idx:{h}", RESULT_TTL)

    # ── 查询 ──

    def get(self, job_id: str) -> dict | None:
        job = self._r.hgetall(f"job:{job_id}")
        if not job:
            return None
        out: dict = {
            "job_id": job_id,
            "status": job.get("status", "pending"),
            "error": job.get("error"),
            "payload": None,
        }
        if out["status"] == "done":
            raw = self._r.get(f"job:result:{job_id}")
            if raw:
                out["payload"] = json.loads(raw)
        return out
