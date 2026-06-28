using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using StockData.Mcp.Data;
using StockData.Mcp.Fetching;
using StockData.SyncCli.Logging;
using StockData.SyncCli.Progress;
using StockData.SyncCli.Ui;
using Terminal.Gui.App;

namespace StockData.SyncCli;

/// <summary>
/// sync-cli 入口：手写 arg 解析 + DI 容器 + 三模式分发（drain / market / status / help）。
///
/// 设计：
/// - **不使用 HostBuilder**：WebApplication/WebHost 拉 ASP.NET Core 进来没必要；裸
///   ServiceCollection + 手编 ConfigurationBuilder 已足够，配 MinimalApi 风格的入口；
/// - **强制 PipelineEnabled=true**：CLI 自身存在意义就是同步，不受 MCP 配置开关控制；
/// - **PG DSN 走 STOCKDATA_PG_DSN**：与 MCP / fetch 共用同一份 .env（README 已约定）；
/// - **一次性 drain**：drain 命令默认就是消费 stock_sync_task 直到队列空/fetch halt 后退出
///   （cron 友好）。Ctrl-C 走 CancelKeyPress → cts.Cancel() 让底层 drain 走步级断点收尾。
/// - **TUI 自动关闭**：drain 自然完成时 DashboardWindow 的 finally 会 RequestStop，
///   Program 再 cts.Cancel() 让 halt monitor 干净退出。
/// </summary>
public static class Program
{
    private const string AppName = "stockdata-sync-cli";

    public static async Task<int> Main(string[] args)
    {
        if (args.Length == 0 || IsHelp(args[0]))
        {
            PrintUsage();
            return 0;
        }

        var sub = args[0].ToLowerInvariant();
        switch (sub)
        {
            case "drain":
                return await RunDrainAsync(args[1..]).ConfigureAwait(false);
            case "market":
                return await RunOnceAsync("market").ConfigureAwait(false);
            case "retry":
                return await RunOnceAsync("retry").ConfigureAwait(false);
            case "status":
                return await RunOnceAsync("status").ConfigureAwait(false);
            default:
                Console.Error.WriteLine($"未知子命令：{args[0]}");
                PrintUsage();
                return 2;
        }
    }

    private static bool IsHelp(string s) =>
        s is "--help" or "-h" or "help" or "/?";

    private static void PrintUsage()
    {
        Console.WriteLine($"""
            {AppName} — stockdata 同步后台（独立进程版）

            用法：
              sync-cli drain [--code <code>] [--kind full|minute]
              sync-cli market
              sync-cli retry
              sync-cli status
              sync-cli --help

            子命令：
              drain   一次性消费 stock_sync_task 直到队列空/halt 后退出（cron 友好）。
                     --code 同步单票后退出（仅供测试）。
                     --kind full（默认）/ minute。
              market  同步市场级数据（trade_calendar + 证券列表 + 行业 + 指数成分）后退出。
              retry   把 status='failed' 的任务重置为 pending，下次 drain 会捡起重试。
                     Coverage 幂等保证：已新鲜的数据集跳过，只补仍不新鲜的。
              status  打印同步进度（已纳管票数 + 各状态计数）后退出。

            环境变量：
              STOCKDATA_PG_DSN            PostgreSQL DSN（与 MCP 共用 .env）
              StockData__FetchBase        fetch 微服务地址，默认 http://127.0.0.1:8090
              StockData__PipelineEnabled  强制 true（CLI 自身即同步开关）
              StockData__Sync__* / FetchHaltPollSeconds / FetchRestartCooldownSeconds 见 appsettings.json
            """);
    }

    // ── 一次性 drain ─────────────────────────────────────────────
    private static async Task<int> RunDrainAsync(string[] args)
    {
        string? singleCode = null;
        var kind = "full";

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--code" when i + 1 < args.Length:
                    singleCode = args[++i];
                    break;
                case "--kind" when i + 1 < args.Length:
                    kind = args[++i].ToLowerInvariant();
                    if (kind is not ("full" or "minute"))
                    {
                        Console.Error.WriteLine($"未知 kind：{args[i]}（仅 full/minute）");
                        return 2;
                    }
                    break;
                default:
                    Console.Error.WriteLine($"未知参数：{args[i]}");
                    return 2;
            }
        }

        // 关键：TTY 检测必须在 BuildServices 之前完成 —— logger 注册策略依 mode 而定
        // （Tui 模式走 TuiLoggerProvider 写入 ring buffer，避开 Console.Out；Log 模式走 SimpleConsole）。
        var mode = ConsoleModeDetector.Detect();
        using var sp = BuildServices(mode).BuildServiceProvider();
        var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("sync-cli");

        // 单票命令式 → 走 StockSyncService 一次
        if (singleCode is not null)
        {
            var stockSync = sp.GetRequiredService<StockSyncService>();
            try
            {
                var outcome = kind == "minute"
                    ? await stockSync.SyncMinuteAsync(singleCode).ConfigureAwait(false)
                    : await stockSync.SyncStockAsync(singleCode).ConfigureAwait(false);
                Console.WriteLine(JsonSerializer.Serialize(outcome, JsonOpts));
                return outcome.Status == "failed" ? 1 : 0;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "单票同步失败：{Code}", singleCode);
                return 1;
            }
        }

        // 一次性 drain：Tui 走交互面板（ring-buffer 日志 + dashboard）；headless 走 Log 模式（SimpleConsole）。
        if (mode == ConsoleMode.Tui)
        {
            logger.LogInformation("启动 drain（mode={Mode}）", mode);
            return await RunTuiModeAsync(sp, logger).ConfigureAwait(false);
        }
        logger.LogInformation("启动 drain（mode={Mode}）", mode);

        // 关键：Resolve LogProgressSink 让其订阅 ProgressSource（构造即订阅）
        using var sink = sp.GetRequiredService<LogProgressSink>();
        var engine = sp.GetRequiredService<SyncEngine>();
        var haltMonitor = sp.GetRequiredService<HaltMonitor>();

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;          // 阻止立刻终止，让 finally 跑完
            try { cts.Cancel(); } catch (ObjectDisposedException) { }
            logger.LogInformation("收到 Ctrl-C，开始优雅退出…");
        };

        try
        {
            // 平行：drain 主循环 + halt 监视。drain 自然结束（队列空/halt）→ cancel cts → halt 退出。
            var drain = engine.RunAsync(cts.Token);
            var halt = haltMonitor.RunAsync(cts.Token);
            await drain.ConfigureAwait(false);          // 等 drain 自然完成或 Ctrl-C
            try { cts.Cancel(); } catch (ObjectDisposedException) { }
            try { await halt.ConfigureAwait(false); } catch (OperationCanceledException) { /* 正常 */ }
            logger.LogInformation("drain 退出");
            return 0;
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("drain 被取消（Cancelled）");
            return 0;
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "drain 致命错误");
            return 1;
        }
    }

    // ── TUI 模式 ─────────────────────────────────────────────────
    /// <summary>
    /// Terminal.Gui v2 交互面板模式：阻塞运行 app.Run(dashboard)，drain + halt 平行的两个 Task
    /// 在 dashboard.StartDrainLoop 里 Task.Run 出去。Ctrl-C → CancelKeyPress →
    /// cts.Cancel() + app.RequestStop() → dashboard IsRunningChanged 注销订阅 →
    /// app.Dispose 还原终端 → finally 等待 drain task 跑完步级断点收尾。
    ///
    /// 注意：**不在 DI 注册 IApplication** —— Terminal.Gui v2 一进程只允许一个实例（互斥
    /// 状态机），headless 单次命令（market/status）也不需要；放 DI 会污染其它模式的容器。
    /// </summary>
    private static async Task<int> RunTuiModeAsync(IServiceProvider sp, ILogger logger)
    {
        var progress = sp.GetRequiredService<IProgressSource>();
        var engine = sp.GetRequiredService<SyncEngine>();
        var haltMonitor = sp.GetRequiredService<HaltMonitor>();
        var config = sp.GetRequiredService<IConfiguration>();
        var dashboardLogger = sp.GetRequiredService<ILoggerFactory>().CreateLogger<DashboardWindow>();

        using var cts = new CancellationTokenSource();
        var drainLoop = (CancellationToken ct) => Task.WhenAll(
            engine.RunAsync(ct),
            haltMonitor.RunAsync(ct));

        // 关键：Terminal.Gui v2 实例化模型 — Application.Create() / Init() / Run() / Dispose()
        using IApplication app = Application.Create();
        app.Init();

        Console.CancelKeyPress += (_, e) =>
        {
            // 阻止立刻终止，让 finally 跑完
            e.Cancel = true;
            try { cts.Cancel(); } catch (ObjectDisposedException) { }
            try { app.RequestStop(); } catch (ObjectDisposedException) { /* 已关 */ }
            logger.LogInformation("收到 Ctrl-C，开始优雅退出…");
        };

        try
        {
            // TuiLoggerProvider 在 TUI 模式由 BuildServices 注入 DI；headless 模式则为 null。
            var tuiLogger = sp.GetService<TuiLoggerProvider>();
            var syncRun = sp.GetRequiredService<SyncRunService>();
            var fetchControl = sp.GetRequiredService<IFetchControl>();
            var dashboard = new DashboardWindow(
                app, progress, cts, config, dashboardLogger,
                sp, syncRun, fetchControl,
                tuiLogger);
            // 启后台 drain（Task.Run 走线程池，不阻塞 UI 主循环）
            dashboard.StartDrainLoop(drainLoop);
            // 阻塞直到 RequestStop / 用户按 Enter / Ctrl-C / drain 自然完成
            app.Run(dashboard);
            // drain 自然完成 → DashboardWindow.StartDrainLoop 的 finally 已 RequestStop；
            // 此处确保 cts 被取消，让 halt monitor 退出（它的 token 来自 cts）。
            try { cts.Cancel(); } catch (ObjectDisposedException) { }
            logger.LogInformation("dashboard 退出");
            return 0;
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("drain 被取消（Cancelled）");
            return 0;
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "TUI 模式致命错误");
            return 1;
        }
    }

    // ── 一次性命令（market / retry / status）─────────────────────
    private static async Task<int> RunOnceAsync(string kind)
    {
        // market/retry/status 没有 drain 循环 → 强制 Log 模式（结构化 stdout 供 pipe/journald 消费）。
        using var sp = BuildServices(ConsoleMode.Log).BuildServiceProvider();
        var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("sync-cli");

        try
        {
            if (kind == "status")
            {
                var run = sp.GetRequiredService<SyncRunService>();
                var status = await run.StatusAsync().ConfigureAwait(false);
                Console.WriteLine(JsonSerializer.Serialize(status, JsonOpts));
                return 0;
            }
            if (kind == "retry")
            {
                var run = sp.GetRequiredService<SyncRunService>();
                var result = await run.RetryFailedAsync().ConfigureAwait(false);
                Console.WriteLine(JsonSerializer.Serialize(result, JsonOpts));
                return 0;
            }
            // market
            var market = sp.GetRequiredService<SyncMarketService>();
            var m = await market.SyncMarketAsync().ConfigureAwait(false);
            Console.WriteLine(JsonSerializer.Serialize(m, JsonOpts));
            return 0;
        }
        catch (Exception ex) when (ex is Npgsql.NpgsqlException
                                  || ex.InnerException is Npgsql.NpgsqlException
                                  || ex is System.Data.Common.DbException
                                  || ex is HttpRequestException)
        {
            logger.LogError(ex, "{Kind} 命令失败（通常是 PG / fetch 不可达）：{Msg}", kind, ex.Message);
            Console.Error.WriteLine($"错误：{kind} 失败 — {ex.Message}");
            return 1;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "{Kind} 命令未预期异常", kind);
            Console.Error.WriteLine($"错误：{kind} 失败 — {ex.Message}");
            return 1;
        }
    }

    // ── 容器构造 ─────────────────────────────────────────────────
    private static IServiceCollection BuildServices(ConsoleMode mode)
    {
        var config = BuildConfig();

        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(config);

        // TuiLoggerProvider 必须在 AddLogging 闭包外可见（AddProvider 拿到实例 + dashboard 解析它渲染 log 面板），
        // 因此在闭包外声明、闭包内赋值、最后按需注册为单例。
        TuiLoggerProvider? tuiLogger = null;
        services.AddLogging(b =>
        {
            b.SetMinimumLevel(LogLevel.Information);

            // 过滤 EF Core / Http / DI 启动期的 info 噪声（每条 SQL、每个 HTTP 请求一行 → 刷屏）
            b.AddFilter("Microsoft.EntityFrameworkCore", LogLevel.Warning);
            b.AddFilter("Microsoft.EntityFrameworkCore.Database.Command", LogLevel.Warning);
            b.AddFilter("Microsoft.Extensions.DependencyInjection", LogLevel.Warning);
            b.AddFilter("System.Net.Http", LogLevel.Warning);
            b.AddFilter("System.Net.Http.HttpClient", LogLevel.Warning);
            b.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);

            if (mode == ConsoleMode.Tui)
            {
                // TUI 模式：AddSimpleConsole 写 Console.Out → 与 Terminal.Gui 抢 ANSI 流 → 渲染错位。
                // 改用 TuiLoggerProvider 把日志灌进 ring buffer，DashboardWindow 的 log 面板展示。
                tuiLogger = new TuiLoggerProvider();
                b.AddProvider(tuiLogger);
            }
            else
            {
                // Log 模式：stdout 结构化日志（管道 / journald / docker logs 消费）
                b.AddSimpleConsole(o =>
                {
                    o.SingleLine = true;
                    o.TimestampFormat = "HH:mm:ss ";
                });
            }
        });

        if (tuiLogger is not null)
            services.AddSingleton(tuiLogger);   // dashboard 从 DI 拿到它来渲染 log 面板

        // CLI 自身即同步：强制覆盖 PipelineEnabled=false 的情况
        // （MCP 默认关管线；CLI 不论宿主配置如何，开机即接管同步）
        var finalConfig = new ConfigurationBuilder()
            .AddConfiguration(config)
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["StockData:PipelineEnabled"] = "true",
            })
            .Build();
        services.RemoveAll<IConfiguration>();
        services.AddSingleton<IConfiguration>(finalConfig);

        // 复用 MCP 的接线（DbContext / IFetchClient / SyncRunService / 等）
        services.AddStockDataPipeline(finalConfig);

        // CLI 自己的注入：SyncDrainer + FetchHaltMonitor 由 CLI 显式注册为单例
        services.AddSingleton<SyncDrainer>();
        services.AddSingleton<FetchHaltMonitor>();
        services.AddSingleton<SyncEngine>();
        services.AddSingleton<HaltMonitor>();
        services.AddSingleton<IProgressSource, ProgressSource>();
        services.AddSingleton<LogProgressSink>();
        return services;
    }

    private static IConfiguration BuildConfig()
    {
        var dsn = Environment.GetEnvironmentVariable("STOCKDATA_PG_DSN");
        // appsettings.json + 环境变量（StockData__*）；env var 优先级最高
        var cb = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
            .AddEnvironmentVariables();
        var cfg = cb.Build();
        if (!string.IsNullOrWhiteSpace(dsn))
            cfg["StockData:PgDsn"] = dsn;
        return cfg;
    }

    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
    };
}