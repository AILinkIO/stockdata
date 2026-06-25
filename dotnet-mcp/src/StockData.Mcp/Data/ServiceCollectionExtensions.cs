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
        services.TryAddSingleton(TimeProvider.System);

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

        return services;
    }
}
