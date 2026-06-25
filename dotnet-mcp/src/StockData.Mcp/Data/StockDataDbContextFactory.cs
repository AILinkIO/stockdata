using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace StockData.Mcp.Data;

/// <summary>
/// 设计期工厂：供 `dotnet ef migrations` / `database update` 构造上下文。
/// 复用现有 <c>STOCKDATA_PG_DSN</c>（与 server/.env 同一变量，postgresql+psycopg URL），
/// 转成 Npgsql 连接串。未设置时退回本机无密码默认（仅离线生成迁移用，不连库）。
/// </summary>
public sealed class StockDataDbContextFactory : IDesignTimeDbContextFactory<StockDataDbContext>
{
    public StockDataDbContext CreateDbContext(string[] args)
    {
        var dsn = Environment.GetEnvironmentVariable("STOCKDATA_PG_DSN");
        var conn = string.IsNullOrWhiteSpace(dsn)
            ? "Host=127.0.0.1;Port=5432;Database=stockdata;Username=stockdata"
            : ToNpgsql(dsn);

        var options = new DbContextOptionsBuilder<StockDataDbContext>()
            .UseNpgsql(conn)
            .Options;
        return new StockDataDbContext(options);
    }

    /// <summary>postgresql+psycopg://user:pass@host:port/db → Npgsql 连接串。</summary>
    internal static string ToNpgsql(string dsn)
    {
        var scheme = dsn.IndexOf("://", StringComparison.Ordinal);
        var rest = scheme >= 0 ? dsn[(scheme + 3)..] : dsn;          // user:pass@host:port/db

        var slash = rest.IndexOf('/');
        var authority = slash >= 0 ? rest[..slash] : rest;
        var db = slash >= 0 ? rest[(slash + 1)..] : "";
        var query = db.IndexOf('?');
        if (query >= 0) db = db[..query];

        var at = authority.LastIndexOf('@');
        var userInfo = at >= 0 ? authority[..at] : "";
        var hostPort = at >= 0 ? authority[(at + 1)..] : authority;

        var colon = userInfo.IndexOf(':');
        var user = colon >= 0 ? userInfo[..colon] : userInfo;
        var pass = colon >= 0 ? userInfo[(colon + 1)..] : "";

        var hpColon = hostPort.LastIndexOf(':');
        var host = hpColon >= 0 ? hostPort[..hpColon] : hostPort;
        var port = hpColon >= 0 ? hostPort[(hpColon + 1)..] : "5432";

        var parts = new List<string> { $"Host={host}", $"Port={port}", $"Database={db}", $"Username={user}" };
        if (pass.Length > 0) parts.Add($"Password={pass}");
        return string.Join(";", parts);
    }
}
