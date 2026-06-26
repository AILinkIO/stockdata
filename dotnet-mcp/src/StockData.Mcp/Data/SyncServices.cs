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
/// 不在 halt 期间硬刚（§0）。串行逐查询提交，配速由 fetch 微服务 60/min 限流兜住。
/// 单例：自建 DI scope（Scoped 的 DbContext/各 Service）。
/// </summary>
public sealed class StockSyncService(IServiceProvider root, IConfiguration config)
{
    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");

    public async Task<SyncOutcome> SyncStockAsync(string code, CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var now = sp.GetRequiredService<TimeProvider>().GetUtcNow();
        var today = Coverage.Today(now);

        // 确保 synced_stock + task 行存在（幂等），再加载已存的进度（含上轮 partial 的 datasets_done）
        await SyncRegistry.RegisterIfNewAsync(db, code, ct);
        var task = await db.StockSyncTasks.FirstAsync(t => t.Code == code && t.Kind == "full", ct);
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
            var snaps = sp.GetRequiredService<SnapshotService>();
            await Step("stock_basic", () => snaps.EnsureSnapshotAsync(new StockBasicIngest(db, code), today, now, ct));

            await Step("k_d", () => sp.GetRequiredService<KlineService>()
                .EnsureRangeAsync(code, "k_d", Coverage.BackfillStart("k_d"), today, now, ct));

            await Step("adjust_factor", () => sp.GetRequiredService<AdjustFactorService>()
                .EnsureFullAsync(code, today, now, ct));

            // dividend/financial 按年/季遍历，下限取上市年（无则 A 股 epoch），上限今年
            var basic = await db.StockBasics.AsNoTracking().FirstOrDefaultAsync(x => x.Code == code, ct);
            var floorYear = basic?.IpoDate?.Year ?? Coverage.BackfillStart("k_d").Year;

            await Step("dividend", async () =>
            {
                var div = sp.GetRequiredService<DividendService>();
                for (var y = floorYear; y <= today.Year; y++)
                    await div.EnsureAsync(code, y, "report", now, ct);
            });

            await Step("financial", async () =>
            {
                var fin = sp.GetRequiredService<FinancialQuarterService>();
                for (var y = floorYear; y <= today.Year; y++)
                    for (var q = 1; q <= 4; q++)
                    {
                        if (new DateOnly(y, (q - 1) * 3 + 1, 1) > today) break;
                        await fin.EnsureAsync(code, y, q, now, ct);
                    }
            });

            await Step("performance", async () =>
            {
                var perf = sp.GetRequiredService<PerformanceService>();
                var pStart = new DateOnly(floorYear, 1, 1);
                await perf.EnsureAsync(code, "express", pStart, today, now, ct);
                await perf.EnsureAsync(code, "forecast", pStart, today, now, ct);
            });

            task.Status = "done";
            task.FinishedAt = now;
            task.Error = null;
        }
        catch (Exception ex) when (ex is FetchTimeoutException or FetchFailedException)
        {
            // 抓取失败（多为 baostock 熔断 halt）：保留已完成步，标 partial 待续，不硬刚
            task.Status = "partial";
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
/// 命令式批量续传（TASK 本轮 D-4，/sync/run 由外部 cron 调）：扫 stock_sync_task 取
/// pending / partial（中断待续）/ done 但已过期（finished_at 早于 StaleAfterHours）的票，
/// 逐票 SyncStockAsync。**遇 partial（多为 halt）即停**，下轮再续，不在 halt 期硬刚（§0）。
/// </summary>
public sealed class SyncRunService(IServiceProvider root, IConfiguration config, StockSyncService stockSync)
{
    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");

    public async Task<object> RunAsync(int max, CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<StockDataDbContext>();
        var now = scope.ServiceProvider.GetRequiredService<TimeProvider>().GetUtcNow();
        var staleHours = config.GetValue("StockData:Sync:StaleAfterHours", 20);
        var cutoff = now.AddHours(-staleHours);

        var codes = await db.StockSyncTasks.AsNoTracking()
            .Where(t => t.Kind == "full" && (t.Status == "pending" || t.Status == "partial"
                        || (t.Status == "done" && t.FinishedAt < cutoff)))
            .OrderBy(t => t.UpdatedAt)
            .Select(t => t.Code)
            .Take(max)
            .ToListAsync(ct);

        int done = 0, partial = 0, failed = 0;
        var stopped = false;
        foreach (var code in codes)
        {
            if (ct.IsCancellationRequested) break;
            var o = await stockSync.SyncStockAsync(code, ct);
            if (o.Status == "done") done++;
            else if (o.Status == "partial") { partial++; stopped = true; break; }  // halt → 停，下轮续
            else failed++;
        }
        return new { processed = done + partial + failed, done, partial, failed, candidates = codes.Count, stopped };
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
