using Microsoft.EntityFrameworkCore;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>单票同步结果（回给命令端点 / RunService 汇总）。</summary>
public sealed record SyncOutcome(string Code, string Status, string[] Done, string? Error);

/// <summary>
/// 单票“同步所有数据”编排（TASK 本轮 P2，命令式 + 断点续传）。
///
/// 顺序复用现成 EnsureXxxAsync（stock_basic→k_d→adjust→dividend→financial→performance），
/// 每步幂等（Coverage：Fresh 跳过）→ 天然续传。<see cref="StockSyncTask.DatasetsDone"/> 作粗粒度
/// 快跳、每步完成即落库（细到步级断点）；data_watermark 是数据集级断点。
/// 抓取失败（fetch 超时/失败，多因 baostock 熔断 halt）→ 标 partial 退出，下轮 /sync/run 续传，
/// 不在 halt 期间硬刚（§0）。串行逐查询提交，配速由 fetch 微服务 90/min 限流兜住。
/// 单例：自建 DI scope（Scoped 的 DbContext/各 Service）。
/// </summary>
public sealed class StockSyncService(IServiceProvider root, IConfiguration config)
{
    /// <summary>同步步骤体：用注入的 step(name, action) 逐步执行（已完成步自动跳过 + 落断点）。</summary>
    private delegate Task SyncWork(IServiceProvider sp, StockDataDbContext db, DateTimeOffset now, DateOnly today,
        Func<string, Func<Task>, Task> step);

    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");

    /// <summary>全数据集同步（kind=full）：stock_basic→k_d→adjust→dividend→financial→performance。</summary>
    public Task<SyncOutcome> SyncStockAsync(string code, CancellationToken ct = default)
        => RunTaskAsync(code, "full", async (sp, db, now, today, step) =>
        {
            await step("stock_basic", () => sp.GetRequiredService<SnapshotService>()
                .EnsureSnapshotAsync(new StockBasicIngest(db, code), today, now, ct));

            await step("k_d", () => sp.GetRequiredService<KlineService>()
                .EnsureRangeAsync(code, "k_d", Coverage.BackfillStart("k_d"), today, now, ct));

            await step("adjust_factor", () => sp.GetRequiredService<AdjustFactorService>()
                .EnsureFullAsync(code, today, now, ct));

            // dividend/financial 按年/季遍历，下限取上市年（无则 A 股 epoch），上限今年
            var basic = await db.StockBasics.AsNoTracking().FirstOrDefaultAsync(x => x.Code == code, ct);
            var floorYear = basic?.IpoDate?.Year ?? Coverage.BackfillStart("k_d").Year;

            await step("dividend", async () =>
            {
                var div = sp.GetRequiredService<DividendService>();
                for (var y = floorYear; y <= today.Year; y++)
                    await div.EnsureAsync(code, y, "report", now, ct);
            });

            await step("financial", async () =>
            {
                var fin = sp.GetRequiredService<FinancialQuarterService>();
                for (var y = floorYear; y <= today.Year; y++)
                    for (var q = 1; q <= 4; q++)
                    {
                        if (new DateOnly(y, (q - 1) * 3 + 1, 1) > today) break;
                        await fin.EnsureAsync(code, y, q, now, ct);
                    }
            });

            await step("performance", async () =>
            {
                var perf = sp.GetRequiredService<PerformanceService>();
                var pStart = new DateOnly(floorYear, 1, 1);
                await perf.EnsureAsync(code, "express", pStart, today, now, ct);
                await perf.EnsureAsync(code, "forecast", pStart, today, now, ct);
            });
        }, ct);

    /// <summary>分钟线同步（kind=minute，TASK 本轮 P3，显式下达）：k_5/15/30/60 全历史（floor=2023-01-01）。</summary>
    public Task<SyncOutcome> SyncMinuteAsync(string code, CancellationToken ct = default)
        => RunTaskAsync(code, "minute", async (sp, db, now, today, step) =>
        {
            await SyncRegistry.EnableMinuteAsync(db, code, ct);   // 标记该票纳管分钟线
            var svc = sp.GetRequiredService<KlineMinuteService>();
            foreach (short f in (short[])[5, 15, 30, 60])
                await step($"k_{f}", () => svc.EnsureRangeAsync(code, f, Coverage.MinuteBackfillStart, today, now, ct));
        }, ct);

    /// <summary>
    /// 任务生命周期骨架（full/minute 共用）：登记 → 加载/新建 task(kind) → running → 逐步执行
    /// （每步幂等 + 落 datasets_done 步级断点）→ done/partial/failed。抓取失败(多为 halt)→ partial 保进度。
    /// </summary>
    private async Task<SyncOutcome> RunTaskAsync(string code, string kind, SyncWork work, CancellationToken ct)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var now = sp.GetRequiredService<TimeProvider>().GetUtcNow();
        var today = Coverage.Today(now);

        await SyncRegistry.RegisterIfNewAsync(db, code, ct);   // 确保 synced_stock + full task 存在
        var task = await db.StockSyncTasks.FirstOrDefaultAsync(t => t.Code == code && t.Kind == kind, ct);
        if (task is null)   // minute task（或直接命令的新票）按需建
        {
            task = new StockSyncTask { Code = code, Kind = kind, Status = "pending", DatasetsDone = [], RequestedAt = now };
            db.StockSyncTasks.Add(task);
        }
        // 仅 partial（上轮中断）才续传保留 datasets_done；pending / 过期 done 都是新一轮 → 清空重走
        // （Coverage 仍会跳过已新鲜的数据集，新一轮成本低但能刷新；否则 datasets_done 满会假装完成不刷新）
        var resume = task.Status == "partial";
        if (!resume) task.DatasetsDone = [];
        task.Status = "running";
        task.StartedAt = now;
        task.Attempt += 1;
        task.Error = null;
        await SaveAsync(db, task, now, ct);

        var done = new HashSet<string>(task.DatasetsDone);
        async Task Step(string name, Func<Task> action)
        {
            if (done.Contains(name)) return;
            await action();
            done.Add(name);
            task.DatasetsDone = done.ToArray();   // 每步完成即落库 → 步级断点
            await SaveAsync(db, task, now, ct);
        }

        try
        {
            await work(sp, db, now, today, Step);
            task.Status = "done";
            task.FinishedAt = now;
            task.Error = null;
        }
        catch (Exception ex) when (ex is FetchTimeoutException or FetchFailedException)
        {
            task.Status = "partial";   // 抓取失败（多为 baostock 熔断 halt）：保留已完成步待续，不硬刚
            task.Error = Trunc(ex.Message);
        }
        catch (Exception ex)
        {
            task.Status = "failed";
            task.Error = Trunc(ex.Message);
        }

        task.DatasetsDone = done.ToArray();
        await SaveAsync(db, task, now, ct);
        return new SyncOutcome(code, task.Status, task.DatasetsDone, task.Error);
    }

    private static async Task SaveAsync(StockDataDbContext db, StockSyncTask task, DateTimeOffset now, CancellationToken ct)
    {
        task.UpdatedAt = now;   // updated_at 的 now() 默认仅 INSERT 生效，UPDATE 需显式
        await db.SaveChangesAsync(ct);
    }

    private static string Trunc(string s) => s.Length <= 480 ? s : s[..480];
}

/// <summary>
/// 市场级数据同步（无单票 code，TASK 本轮 D-2，/sync/market 由 cron 每日先调）：
/// trade_calendar（日期运算刚需）→ stock_list → industry → index 成分（sz50/hs300/zz500）。
/// </summary>
public sealed class SyncMarketService(IServiceProvider root, IConfiguration config)
{
    private static readonly string[] Indexes = { "sz50", "hs300", "zz500" };

    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");

    public async Task<object> SyncMarketAsync(CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var now = sp.GetRequiredService<TimeProvider>().GetUtcNow();
        var today = Coverage.Today(now);
        try
        {
            // 日历：补到去年初~今年末（覆盖跨年 + 当年全假期）
            await sp.GetRequiredService<TradeCalendarService>()
                .EnsureRangeAsync(new DateOnly(today.Year - 1, 1, 1), new DateOnly(today.Year, 12, 31), now, ct);

            var sd = await db.TradeCalendars.AsNoTracking()
                .Where(c => c.CalendarDate <= today && c.IsTradingDay)
                .OrderByDescending(c => c.CalendarDate).Select(c => (DateOnly?)c.CalendarDate).FirstOrDefaultAsync(ct);
            if (sd is not DateOnly snap)
                return new { status = "failed", error = "无交易日历数据，无法确定快照日" };

            var snaps = sp.GetRequiredService<SnapshotService>();
            await snaps.EnsureSnapshotAsync(new StockListIngest(db), snap, now, ct);
            await snaps.EnsureSnapshotAsync(new IndustryIngest(db), snap, now, ct);
            foreach (var idx in Indexes)
                await snaps.EnsureSnapshotAsync(new IndexConstituentIngest(db, idx), snap, now, ct);

            return new { status = "done", snap_date = snap.ToString("yyyy-MM-dd") };
        }
        catch (Exception ex) when (ex is FetchTimeoutException or FetchFailedException)
        {
            return new { status = "partial", error = ex.Message };
        }
    }
}

/// <summary>
/// 同步控制面（cron 调，均快返回、不抓 baostock）：消费由常驻 <see cref="SyncDrainer"/> 负责。
/// </summary>
public sealed class SyncRunService(IServiceProvider root, IConfiguration config)
{
    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");

    /// <summary>
    /// cron「生成队列」：把上次完成早于 StaleAfterHours 的 done 任务重置为 pending，交 Drainer 后台消费。
    /// 立即返回（只一条 UPDATE，不碰 baostock）。pending/partial 本就在队列里，无需触碰。
    /// </summary>
    public async Task<object> RefreshAsync(CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<StockDataDbContext>();
        var now = scope.ServiceProvider.GetRequiredService<TimeProvider>().GetUtcNow();
        var staleHours = config.GetValue("StockData:Sync:StaleAfterHours", 20);
        var cutoff = now.AddHours(-staleHours);
        var requeued = await db.Database.ExecuteSqlRawAsync(
            "UPDATE stock_sync_task SET status = 'pending', updated_at = now() WHERE status = 'done' AND finished_at < {0}",
            new object[] { cutoff }, ct);
        return new { requeued, cutoff_hours = staleHours };
    }

    /// <summary>同步进度观测：按状态计数 + 已纳管票数。</summary>
    public async Task<object> StatusAsync(CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<StockDataDbContext>();
        var byStatus = await db.StockSyncTasks.AsNoTracking()
            .GroupBy(t => t.Status)
            .Select(g => new { status = g.Key, count = g.Count() })
            .ToListAsync(ct);
        var registered = await db.SyncedStocks.AsNoTracking().CountAsync(ct);
        return new { registered, tasks = byStatus };
    }
}
