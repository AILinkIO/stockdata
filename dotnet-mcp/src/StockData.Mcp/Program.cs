using Microsoft.Extensions.Http.Resilience;
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
        o.AttemptTimeout.Timeout = TimeSpan.FromSeconds(100);       // 单次尝试（覆盖 API 60s 等待）
        o.TotalRequestTimeout.Timeout = TimeSpan.FromSeconds(240);  // 总预算
        o.Retry.MaxRetryAttempts = 2;                               // 504/502 重试（届时多已落库）
        o.CircuitBreaker.SamplingDuration = TimeSpan.FromSeconds(200);
    });

builder.Services
    .AddMcpServer()
    .WithHttpTransport()        // Streamable HTTP
    .WithToolsFromAssembly();   // 扫描 [McpServerToolType]

var app = builder.Build();

app.MapMcp("/mcp");
app.MapGet("/healthz", () => Results.Json(new { status = "ok" }));

app.Run("http://0.0.0.0:8000"); // 沿用旧 Python MCP 的端口，存量客户端配置不变
