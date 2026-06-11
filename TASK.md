# fetcher 优化任务（成功率 + 效率）

> 来源：2026-06-11 fetcher 代码分析（背景：baostock 慢网络导致的超时/僵尸任务事故）。
> 状态：⬜ 待办 / 🔄 进行中 / ✅ 完成

## 高价值：成功率

- ✅ **T1 baostock socket 超时**（`fetcher/providers/baostock.py`、`settings.py`）
  `bs.login()` 用 `socket.setdefaulttimeout()` 包住，连接创建时继承超时（默认 30s，
  `STOCKDATA_BAOSTOCK_SOCKET_TIMEOUT` 可覆盖）；登录阶段网络异常包装为 `LoginError`；
  `TimeoutError` 加入可重试判定。效果：挂死从"90s SIGKILL→僵尸行"变为
  "30s 超时→进程内重连重试"，SIGKILL 退化为兜底。

- ✅ **T2 `worker_prefetch_multiplier=1`**（`fetcher/app.py`）
  `acks_late` + 默认预取 4：连续慢任务时被预取消息 unacked 超过
  `visibility_timeout(600s)` 会被重复投递执行。预取 1 是 acks_late 的标准搭配。

- ✅ **T3 软超时不做进程内重试**（`fetcher/providers/baostock.py`、`fetcher/tasks.py`）
  `_query` 显式放行 `SoftTimeLimitExceeded`（剩余预算不足，重试注定被硬杀）；
  `_TASK_OPTS.autoretry_for` 加入该异常，交给 Celery 任务级重试（全新时限）。

## 中价值：效率 + 观测

- ✅ **T4 beat 投递走去重**（`fetcher/beat.py`）
  `app.send_task` 改为 `dispatch.submit()`：与读穿透并发时不重复抓取，
  且定时任务在 `fetch_task` 有流水可查。

- ✅ **T5 超大范围回填切片**（`api/services/readthrough.py` + 单元测试）
  缺口超过上限时切成多段顺序任务（日/周/月线与复权因子 10 年/段，分钟线 2 年/段）：
  单任务时长有界，每段落库即推进水位，失败重试只补剩余段（断点续传）。

- ✅ **T6 `fetch_task.params` GIN 索引**（alembic 迁移）
  `ensure_quarter` 的 `params @> {...}` 点查目前全表扫，`fetch_task` 只进不出，
  半年后会变慢。不做清理（财报负结果记忆依赖成功记录），加索引解决。

## 收尾

- ✅ **T7 文档同步**：`docs/data-lifecycle.md` 第 2 节补充切片回填行为。
- ✅ **T8 验证**：单测全过；重建栈；新 code 全史回填实际产生多段任务并成功。

## 明确不做（分析结论）

- 闲置 60s 预防性重登录的移除——T1 落地后它变冗余，但现在工作正常，留待观察。
- writer 逐行转换优化——纯 CPU 毫秒级，瓶颈在网络。
- 扩分片数——当前 8 个跟踪标的远未到瓶颈，跟踪数百 code 时再调 `worker_shards`。
