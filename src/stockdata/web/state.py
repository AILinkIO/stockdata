"""进程级共享状态：唯一的 SyncRunner 实例（web 页面与 REST API 共用）。"""

from __future__ import annotations

from stockdata.sync.runner import SyncRunner

runner: SyncRunner | None = None


def get_runner() -> SyncRunner:
    if runner is None:
        raise RuntimeError("SyncRunner 未初始化（app 未启动）")
    return runner
