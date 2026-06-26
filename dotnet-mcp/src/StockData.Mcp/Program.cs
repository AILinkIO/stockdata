using Microsoft.Extensions.Http.Resilience;
using StockData.Mcp.Data;
using StockData.Mcp.StockDataClient;

var builder = WebApplication.CreateBuilder(args);

// REST API typed client：读穿透首次触达可达数十秒，超时与重试预算放宽
var apiBase = builder.Configuration["StockData:ApiBase"] ?? "http://127.0.0.1:8080";
builder.Services.AddHttpClient<StockDataApiClient>(c =>
    {
        c.BaseAddress = new Uri(apiBase);
        c.Timeout = Timeout.InfiniteTimeSpan; // 超时统一交给 resilience 管道
    })
    .AddStandardResilienceHandler(o =>
    {
        o.AttemptTimeout.Timeout = TimeSpan.FromSeconds(150);       // 单次尝试（覆盖 API 120s 等待）
        o.TotalRequestTimeout.Timeout = TimeSpan.FromSeconds(360);  // 总预算
        o.Retry.MaxRetryAttempts = 2;                               // 504/502 重试（届时多已落库）
        o.CircuitBreaker.SamplingDuration = TimeSpan.FromSeconds(300);
    });

builder.Services.AddMemoryCache();

// dotnet 数据管线（迁移中）：默认关闭，开启需 StockData:PipelineEnabled=true。
// KlineReadService 始终注册（仅依赖 IServiceProvider/IConfiguration），其 Enabled 反映开关；
// 关闭时工具不调用它、走旧 REST → 现网行为与今日完全一致。重服务（DbContext/KlineService/
// HttpFetchClient）仅开启时注册。
builder.Services.AddSingleton<StockData.Mcp.Data.KlineReadService>();
builder.Services.AddSingleton<StockData.Mcp.Data.KlineMinuteReadService>();
builder.Services.AddSingleton<StockData.Mcp.Data.TradeCalendarReadService>();
builder.Services.AddSingleton<StockData.Mcp.Data.StockBasicReadService>();
builder.Services.AddSingleton<StockData.Mcp.Data.DividendReadService>();
builder.Services.AddSingleton<StockData.Mcp.Data.MacroReadService>();
builder.Services.AddSingleton<StockData.Mcp.Data.FinancialReadService>();
builder.Services.AddSingleton<StockData.Mcp.Data.TradingDaysReadService>();
builder.Services.AddSingleton<StockData.Mcp.Data.SnapshotReadService>();
builder.Services.AddSingleton<StockData.Mcp.Data.AdjustFactorReadService>();
builder.Services.AddSingleton(TimeProvider.System);   // StockAnalysisService 依赖（始终注册）
builder.Services.AddSingleton<StockData.Mcp.Data.StockAnalysisService>();
if (builder.Configuration.GetValue<bool>("StockData:PipelineEnabled"))
    builder.Services.AddStockDataPipeline(builder.Configuration);

builder.Services
    .AddMcpServer()
    .WithHttpTransport()        // Streamable HTTP
    .WithToolsFromAssembly();   // 扫描 [McpServerToolType]

var app = builder.Build();

app.MapMcp("/mcp");
app.MapGet("/healthz", () => Results.Json(new
{
    status = "ok",
    name = "stockdata-mcp",
    version = typeof(Program).Assembly.GetName().Version?.ToString() ?? "0.0.0",
}));

// ── 命令式同步接口（P2，由外部 cron 驱动；管线关闭时 503）──
// 单票全数据同步：POST /sync/stock?code=sh.600000[&minute=true]
app.MapPost("/sync/stock", async (HttpContext http, string code, bool? minute, CancellationToken ct) =>
{
    var sync = http.RequestServices.GetService<StockSyncService>();
    if (sync is null) return Results.Json(new { error = "pipeline disabled (StockData:PipelineEnabled=false)" }, statusCode: 503);
    if (string.IsNullOrWhiteSpace(code)) return Results.Json(new { error = "缺少 code" }, statusCode: 400);
    var norm = CodeNormalizer.ToBaostock(code);
    // minute=true：分钟线全历史（k_5/15/30/60）；否则全数据集
    return Results.Json(minute == true
        ? await sync.SyncMinuteAsync(norm, ct)
        : await sync.SyncStockAsync(norm, ct));
});
// 批量续传：POST /sync/run?max=200（扫 pending/partial/过期票，逐票续传，遇 halt 即停）
app.MapPost("/sync/run", async (HttpContext http, int? max, CancellationToken ct) =>
{
    var run = http.RequestServices.GetService<SyncRunService>();
    if (run is null) return Results.Json(new { error = "pipeline disabled" }, statusCode: 503);
    return Results.Json(await run.RunAsync(max is > 0 ? max.Value : 200, ct));
});
// 市场级数据：POST /sync/market（日历/列表/行业/指数成分，cron 每日先于 /sync/run 调）
app.MapPost("/sync/market", async (HttpContext http, CancellationToken ct) =>
{
    var m = http.RequestServices.GetService<SyncMarketService>();
    if (m is null) return Results.Json(new { error = "pipeline disabled" }, statusCode: 503);
    return Results.Json(await m.SyncMarketAsync(ct));
});
// 进度观测：GET /sync/status
app.MapGet("/sync/status", async (HttpContext http, CancellationToken ct) =>
{
    var run = http.RequestServices.GetService<SyncRunService>();
    if (run is null) return Results.Json(new { error = "pipeline disabled" }, statusCode: 503);
    return Results.Json(await run.StatusAsync(ct));
});

app.Run("http://0.0.0.0:8000"); // 沿用旧 Python MCP 的端口，存量客户端配置不变
