using System.Buffers;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
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
public sealed class KlineReadService(IServiceProvider root, IConfiguration config, ILogger<KlineReadService> logger)
{
    public bool Enabled => config.GetValue<bool>("StockData:PipelineEnabled");
    private bool ServeFromPgOnly => config.GetValue<bool>("StockData:ServeFromPgOnly");

    public async Task<string> GetHistoricalJsonAsync(
        string code, string frequency, DateOnly start, DateOnly end, string adjustFlag, CancellationToken ct = default)
    {
        await using var scope = root.CreateAsyncScope();
        var sp = scope.ServiceProvider;
        var db = sp.GetRequiredService<StockDataDbContext>();
        var tp = sp.GetRequiredService<TimeProvider>();
        var now = tp.GetUtcNow();

        // 方案 A：pgOnly 时登记该票（后台 Drainer 全量同步）+ 水位检查（Fresh→返回/有数据→返回stale/无数据→等Drainer）
        if (ServeFromPgOnly) await SyncRegistry.RegisterIfNewAsync(db, code, ct);

        var needAdjust = adjustFlag == "1" || adjustFlag == "2";

        // K 线与 AdjustFactor 的水位检查/补抓并行执行。DbContext 非线程安全：同一 scope 的实例
        // 不能被并发操作（EF Core 会抛 InvalidOperationException: a second operation was started
        // on this context instance）。故两条路径各自建独立 scope（独立 DbContext 实例）；
        // 主 scope 的 db 仅用于上面的懒登记与下方 WhenAll 之后的串行读取。
        async Task EnsureKlineAsync()
        {
            await using var s = root.CreateAsyncScope();
            var ssp = s.ServiceProvider;
            await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, logger, tp, ct,
                SyncAwaiter.RangeCheck(ssp.GetRequiredService<IWatermarkStore>(),
                    code, $"k_{frequency}", start, end, now),
                c => ssp.GetRequiredService<KlineService>()
                    .EnsureRangeAsync(code, $"k_{frequency}", start, end, now, c));
        }
        async Task EnsureFactorAsync()
        {
            await using var s = root.CreateAsyncScope();
            var ssp = s.ServiceProvider;
            await SyncAwaiter.EnsureAsync(config, ServeFromPgOnly, logger, tp, ct,
                SyncAwaiter.AdjustFactorCheck(ssp.GetRequiredService<IWatermarkStore>(),
                    ssp.GetRequiredService<StockDataDbContext>(), code, now),
                c => ssp.GetRequiredService<AdjustFactorService>().EnsureFullAsync(code, end, now, c));
        }

        if (needAdjust) await Task.WhenAll(EnsureKlineAsync(), EnsureFactorAsync());
        else await EnsureKlineAsync();

        var rows = await db.Klines.AsNoTracking()
            .Where(k => k.Code == code && k.Frequency == frequency
                        && k.TradeDate >= start && k.TradeDate <= end)
            .OrderBy(k => k.TradeDate)
            .ToListAsync(ct);

        // 复权（1 后复权 / 2 前复权）：逐 bar 乘因子（不复权 3 直读原始）
        if (needAdjust && rows.Count > 0)
        {
            var factors = await db.AdjustFactors.AsNoTracking()
                .Where(a => a.Code == code && a.DividOperateDate <= end)
                .OrderBy(a => a.DividOperateDate)
                .ToListAsync(ct);
            AdjustCalc.Apply(rows, factors, adjustFlag);
        }

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
