using System.Buffers;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using StockData.Mcp.Data.Entities;

namespace StockData.Mcp.Data;

/// <summary>
/// 日/周/月 K 线读取（dotnet 落盘路径的对外查询）：EnsureRange → 直读 PG → 序列化为
/// 与旧 Python API 同形状的 JSON 数组（Kline 全列 snake_case，对齐 api/services/kline.get_kline）。
///
/// **始终注册**（仅依赖 IServiceProvider/IConfiguration）：<see cref="Enabled"/> 反映开关，
/// 关闭时 MCP 工具不调用本服务、走旧 REST，故对现网零影响。开启时每次调用自建 DI scope，
/// 解析 DbContext/KlineService（不假设 MCP 是否按请求建 scope）。
/// 仅支持 d/w/m 不复权；复权（adjust_flag 1/2）与分钟线仍由旧 REST 处理。
/// </summary>
public sealed class KlineReadService(IServiceProvider root, IConfiguration config)
{
    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");

    public async Task<string> GetHistoricalJsonAsync(
        string code, string frequency, DateOnly start, DateOnly end, CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var svc = sp.GetRequiredService<KlineService>();
        var time = sp.GetRequiredService<TimeProvider>();

        await svc.EnsureRangeAsync(code, $"k_{frequency}", start, end, time.GetUtcNow(), ct);

        var rows = await db.Klines.AsNoTracking()
            .Where(k => k.Code == code && k.Frequency == frequency
                        && k.TradeDate >= start && k.TradeDate <= end)
            .OrderBy(k => k.TradeDate)
            .ToListAsync(ct);

        return Serialize(rows);
    }

    /// <summary>Kline 列表 → JSON 数组（列序/键名对齐 model_columns(Kline)）。</summary>
    internal static string Serialize(IReadOnlyList<Kline> rows)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buffer))
        {
            w.WriteStartArray();
            foreach (var k in rows)
            {
                w.WriteStartObject();
                w.WriteString("code", k.Code);
                w.WriteString("frequency", k.Frequency);
                w.WriteString("trade_date", k.TradeDate.ToString("yyyy-MM-dd"));
                Dec(w, "open", k.Open);
                Dec(w, "high", k.High);
                Dec(w, "low", k.Low);
                Dec(w, "close", k.Close);
                Dec(w, "preclose", k.Preclose);
                Long(w, "volume", k.Volume);
                Dec(w, "amount", k.Amount);
                Dec(w, "turn", k.Turn);
                Dec(w, "pct_chg", k.PctChg);
                Int(w, "trade_status", k.TradeStatus);
                Bool(w, "is_st", k.IsSt);
                Dec(w, "pe_ttm", k.PeTtm);
                Dec(w, "pb_mrq", k.PbMrq);
                Dec(w, "ps_ttm", k.PsTtm);
                Dec(w, "pcf_ncf_ttm", k.PcfNcfTtm);
                w.WriteString("updated_at", k.UpdatedAt.ToString("o"));
                w.WriteEndObject();
            }
            w.WriteEndArray();
        }
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static void Dec(Utf8JsonWriter w, string name, decimal? v)
    {
        if (v is decimal d) w.WriteNumber(name, d); else w.WriteNull(name);
    }

    private static void Long(Utf8JsonWriter w, string name, long? v)
    {
        if (v is long n) w.WriteNumber(name, n); else w.WriteNull(name);
    }

    private static void Int(Utf8JsonWriter w, string name, short? v)
    {
        if (v is short n) w.WriteNumber(name, n); else w.WriteNull(name);
    }

    private static void Bool(Utf8JsonWriter w, string name, bool? v)
    {
        if (v is bool b) w.WriteBoolean(name, b); else w.WriteNull(name);
    }
}
