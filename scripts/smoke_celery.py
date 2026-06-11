"""
阶段 0 冒烟测试：验证 Python 3.14 + Celery prefork 的四项关键行为。

  1. 任务正常执行与结果返回
  2. task_time_limit 硬超时：子进程被 SIGKILL 且 worker 自动补充新子进程
  3. worker_max_tasks_per_child 到数后子进程被回收重建（PID 轮换）
  4. worker_process_init 信号在子进程内触发（生产中用于 bs.login()）

运行方式（项目根目录）:
    uv run python scripts/smoke_celery.py

依赖 .env 中的 STOCKDATA_BROKER_URL / STOCKDATA_RESULT_BACKEND。
测试开始时会 FLUSHDB 这两个 db（约定为本项目专用的 db2/db3）。
"""

import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from celery import Celery
from celery.exceptions import TimeLimitExceeded
from celery.signals import worker_process_init

from settings import settings

app = Celery("smoke", broker=settings.broker_url, backend=settings.result_backend)
app.conf.update(
    worker_max_tasks_per_child=3,   # 缩小以便快速观察 PID 轮换
    task_time_limit=5,              # 缩小以便快速观察 SIGKILL
    task_soft_time_limit=3,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    broker_transport_options={"visibility_timeout": settings.visibility_timeout},
    result_expires=300,
)

_child_initialized = False  # worker_process_init 是否在本子进程内触发过


@worker_process_init.connect
def _on_child_init(**kwargs):
    """生产环境中此处执行 bs.login()；冒烟测试只立一个标记。"""
    global _child_initialized
    _child_initialized = True


@app.task(name="smoke.pid_task")  # 显式命名：runner 以 __main__ 运行，自动命名会与 worker 侧不一致
def pid_task():
    return {"pid": os.getpid(), "child_initialized": _child_initialized}


@app.task(name="smoke.hang_task")
def hang_task(seconds: int):
    """模拟挂死：无视软超时继续阻塞，逼出 time_limit 的 SIGKILL 路径。"""
    from billiard.exceptions import SoftTimeLimitExceeded

    try:
        time.sleep(seconds)
    except SoftTimeLimitExceeded:
        time.sleep(seconds)
    return "finished"


# ── 以下为 runner：拉起 worker 子进程并逐项验证 ──


def _flush_test_dbs():
    import redis

    for url in (settings.broker_url, settings.result_backend):
        redis.Redis.from_url(url).flushdb()


def _wait_worker_ready(timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if app.control.ping(timeout=1.0):
            return True
    return False


def main() -> int:
    _flush_test_dbs()
    results: dict[str, bool] = {}

    worker = subprocess.Popen(
        [
            sys.executable, "-m", "celery",
            "-A", "scripts.smoke_celery", "worker",
            "--loglevel=warning", "--concurrency=2",
        ],
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    )
    try:
        if not _wait_worker_ready():
            print("FATAL: worker 30s 内未就绪")
            return 1
        print("worker 就绪，开始验证\n")

        # 1 + 4: 正常执行 + init 信号
        r = pid_task.delay().get(timeout=10)
        results["1.任务正常执行与结果返回"] = isinstance(r["pid"], int)
        results["4.worker_process_init 在子进程内触发"] = r["child_initialized"] is True

        # 3: max_tasks_per_child=3，串行投 10 个任务，观察 PID 轮换
        seen = [pid_task.delay().get(timeout=10) for _ in range(10)]
        pids = {x["pid"] for x in seen}
        all_inited = all(x["child_initialized"] for x in seen)
        # concurrency=2、每子进程 3 个任务，11 个任务至少出现 3 个不同 PID
        results["3.max_tasks_per_child 回收重建(PID 轮换)"] = len(pids) >= 3 and all_inited
        print(f"   观察到 {len(pids)} 个不同子进程 PID: {sorted(pids)}")

        # 2: 硬超时 SIGKILL
        t0 = time.monotonic()
        try:
            hang_task.delay(60).get(timeout=30)
            results["2.time_limit 硬超时 SIGKILL"] = False
        except TimeLimitExceeded:
            elapsed = time.monotonic() - t0
            print(f"   挂死任务在 {elapsed:.1f}s 被 SIGKILL（limit=5s）")
            results["2.time_limit 硬超时 SIGKILL"] = elapsed < 15
        except Exception as e:
            print(f"   非预期异常: {type(e).__name__}: {e}")
            results["2.time_limit 硬超时 SIGKILL"] = False

        # 2b: kill 之后池子自动补充，新任务照常执行
        r2 = pid_task.delay().get(timeout=10)
        results["2b.SIGKILL 后 worker 自动补充子进程"] = (
            isinstance(r2["pid"], int) and r2["child_initialized"]
        )
    finally:
        worker.terminate()
        try:
            worker.wait(timeout=10)
        except subprocess.TimeoutExpired:
            worker.kill()
        _flush_test_dbs()

    print("\n══ 冒烟测试结果 ══")
    ok = True
    for name, passed in results.items():
        print(f"  {'PASS' if passed else 'FAIL'}  {name}")
        ok = ok and passed
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
