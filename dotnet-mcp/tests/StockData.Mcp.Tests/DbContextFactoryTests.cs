using StockData.Mcp.Data;

namespace StockData.Mcp.Tests;

/// <summary>DSN → Npgsql 连接串转换（设计期工厂复用 server/.env 的 STOCKDATA_PG_DSN）。</summary>
public class DbContextFactoryTests
{
    [Fact]
    public void 完整_psycopg_dsn_转_npgsql()
        => Assert.Equal(
            "Host=127.0.0.1;Port=5432;Database=stockdata;Username=stockdata;Password=28e3acd8",
            StockDataDbContextFactory.ToNpgsql(
                "postgresql+psycopg://stockdata:28e3acd8@127.0.0.1:5432/stockdata"));

    [Fact]
    public void 无密码_dsn()
        => Assert.Equal(
            "Host=db.local;Port=5432;Database=stockdata;Username=app",
            StockDataDbContextFactory.ToNpgsql("postgresql://app@db.local:5432/stockdata"));

    [Fact]
    public void 缺端口默认5432()
        => Assert.Equal(
            "Host=h;Port=5432;Database=d;Username=u;Password=p",
            StockDataDbContextFactory.ToNpgsql("postgresql+psycopg://u:p@h/d"));

    [Fact]
    public void 忽略查询参数()
        => Assert.Equal(
            "Host=h;Port=5432;Database=d;Username=u",
            StockDataDbContextFactory.ToNpgsql("postgresql://u@h:5432/d?sslmode=require"));
}
