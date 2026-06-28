namespace StockData.SyncCli.Tests;

/// <summary>
/// SyncEngine 测试占位（v1 暂不写）。
///
/// 真正可测的核心是 progress poller（PollLoopAsync + BuildSnapshotAsync）：
///   - 每秒聚合 stock_sync_task 状态计数
///   - L2 当前票 datasets_done 反映到 snapshot
///   - fetch halt 状态查询失败时 poller 不死
///   - ETA 速率推算
///
/// 但 v1 不可测的原因（按"非 refactor / 非 fakes 重型化"约束都不可行）：
///   - Option A：动 SyncDrainer / IFetchControl 让 SyncEngine 接收 fake → 改 MCP 源码（禁）
///   - Option B：抽出 PollLoopAsync 单独测 → refactor（v2 候选）
///   - Option C：全真依赖 + test PG → 退化为集成测试，需 docker-compose（v1 不引入）
///   - Option D：当前选择 — 占位 + 文档化测试缺口
///
/// 实际 poller 行为已由 Phase 0 烟雾测试覆盖：CLI drain 在真 PG / 真 fetch 下产出
/// "[HH:mm:ss] progress: done/total codes ..." 单行日志，对照 task_count 手工核验。
/// v2 重构候选：把 PollLoopAsync / BuildSnapshotAsync 提到独立类并接受 ISyncTaskQuery 接口，
/// 不再依赖 StockDataDbContext 直接查询。
/// </summary>
public class SyncEngineTests
{
    [Fact]
    public void 占位_v2_抽出_progress_query_接口后补单测()
    {
        Assert.True(true);
    }
}
