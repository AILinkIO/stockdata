using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection.Extensions;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>
/// dotnet 数据管线 DI 接线（迁移中）。**默认不注册**——仅当配置 StockData:PipelineEnabled=true
/// 时由 Program 调用，故对现网零影响（现 MCP 工具不解析这些服务，DbContext 也不会连库）。
/// </summary>
public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddStockDataPipeline(this IServiceCollection services, IConfiguration config)
    {
        var dsn = config["StockData:PgDsn"] ?? Environment.GetEnvironmentVariable("STOCKDATA_PG_DSN");
        var conn = string.IsNullOrWhiteSpace(dsn)
            ? "Host=127.0.0.1;Port=5432;Database=stockdata;Username=stockdata"
            : StockDataDbContextFactory.ToNpgsql(dsn);
        services.AddDbContext<StockDataDbContext>(o => o.UseNpgsql(conn));

        services.AddScoped<IWatermarkStore, EfWatermarkStore>();
        services.AddScoped<IKlineWriter, KlineWriter>();
        services.AddScoped<KlineService>();
        services.AddScoped<IKlineMinuteWriter, KlineMinuteWriter>();
        services.AddScoped<KlineMinuteService>();
        services.AddScoped<ITradeCalendarWriter, TradeCalendarWriter>();
        services.AddScoped<TradeCalendarService>();
        services.AddScoped<SnapshotService>();
        services.AddScoped<IAdjustFactorWriter, AdjustFactorWriter>();
        services.AddScoped<IAdjustFactorSignalQuery, EfAdjustFactorSignalQuery>();
        services.AddScoped<AdjustFactorService>();
        services.AddScoped<IDividendWriter, DividendWriter>();
        services.AddScoped<DividendService>();
        services.AddScoped<IMacroWriter, MacroWriter>();
        services.AddScoped<MacroService>();
        services.AddScoped<IFinancialWriter, FinancialWriter>();
        services.AddScoped<FinancialQuarterService>();
        services.AddScoped<PerformanceService>();
        services.TryAddSingleton(TimeProvider.System);

        // 命令式同步编排：单例，各自建 scope（仅管线开启时注册，端点据此 503 判活）
        services.AddSingleton<StockSyncService>();
        services.AddSingleton<SyncMarketService>();
        services.AddSingleton<SyncRunService>();
        // 常驻消费者（方案 A）：唯一串行驱动 baostock 的后台 worker
        services.AddHostedService<SyncDrainer>();

        services.AddSingleton(new FetchClientOptions
        {
            WaitTimeoutSeconds = config.GetValue("StockData:FetchWaitTimeout", 120),
            PollIntervalMs = config.GetValue("StockData:FetchPollMs", 500),
        });

        var fetchBase = config["StockData:FetchBase"]
                        ?? Environment.GetEnvironmentVariable("STOCKDATA_FETCH_BASE")
                        ?? "http://127.0.0.1:8090";
        services.AddHttpClient<IFetchClient, HttpFetchClient>(c =>
        {
            c.BaseAddress = new Uri(fetchBase);
            c.Timeout = Timeout.InfiniteTimeSpan;  // 超时交给 HttpFetchClient 的轮询 deadline
        });

        // fetch 暂停（baostock 拉黑）感知 + 自动恢复：控制面 typed client + 后台监视。
        // 控制面调用短平快（/status、/restart 都即时返回），给短超时而非 Infinite。
        services.AddHttpClient<IFetchControl, FetchControlClient>(c =>
        {
            c.BaseAddress = new Uri(fetchBase);
            c.Timeout = TimeSpan.FromSeconds(10);
        });
        services.AddSingleton(new FetchHaltMonitorOptions
        {
            PollSeconds = config.GetValue("StockData:FetchHaltPollSeconds", 60),
            RestartCooldownSeconds = config.GetValue("StockData:FetchRestartCooldownSeconds", 600),
        });
        services.AddHostedService<FetchHaltMonitor>();

        return services;
    }
}
