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

// ── 同步控制面（方案 A：cron 只「生成队列」秒回，抓取全在常驻 SyncDrainer；管线关闭时 503）──
// 单票同步无独立 URL：由 MCP 读路径在需要时触发（懒登记 + 定向高优先抓取）。
// cron 生成队列：POST /sync/refresh（把过期票重置 pending 交 Drainer 消费，立即返回）
app.MapPost("/sync/refresh", async (HttpContext http, CancellationToken ct) =>
{
    var run = http.RequestServices.GetService<SyncRunService>();
    if (run is null) return Results.Json(new { error = "pipeline disabled" }, statusCode: 503);
    return Results.Json(await run.RefreshAsync(ct));
});
// 市场级数据由常驻 Drainer 按间隔自动维护（无需端点）。
// 进度观测：GET /sync/status
app.MapGet("/sync/status", async (HttpContext http, CancellationToken ct) =>
{
    var run = http.RequestServices.GetService<SyncRunService>();
    if (run is null) return Results.Json(new { error = "pipeline disabled" }, statusCode: 503);
    return Results.Json(await run.StatusAsync(ct));
});

app.Run("http://0.0.0.0:8000"); // 沿用旧 Python MCP 的端口，存量客户端配置不变
