using Microsoft.EntityFrameworkCore;
using Npgsql;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>baostock 复权因子记录 → (除权日, fore, back, adjust)。移植 write_adjust_factor。</summary>
public static class AdjustFactorParser
{
    public static List<(DateOnly Date, decimal Fore, decimal Back, decimal? Adjust)> Parse(FetchPayload p)
    {
        var get = SnapshotSql.Accessor(p);
        var rows = new List<(DateOnly, decimal, decimal, decimal?)>(p.Rows.Count);
        foreach (var r in p.Rows)
        {
            if (KlineParser.Date(get(r, "dividOperateDate")) is not DateOnly d) continue;
            var fore = KlineParser.Dec(get(r, "foreAdjustFactor"));
            var back = KlineParser.Dec(get(r, "backAdjustFactor"));
            if (fore is null || back is null) continue;            // schema 中 fore/back 非空
            rows.Add((d, fore.Value, back.Value, KlineParser.Dec(get(r, "adjustFactor"))));
        }
        return rows;
    }
}

/// <summary>读时复权计算（移植 api/services/kline._factor_at + 应用）。</summary>
public static class AdjustCalc
{
    /// <summary>对 K 线就地复权：flag "2"=前复权(fore)，"1"=后复权(back)；首事件前因子=1。</summary>
    public static void Apply(List<Kline> bars, IReadOnlyList<AdjustFactor> factors, string flag)
    {
        if (factors.Count == 0) return;
        var dates = factors.Select(f => f.DividOperateDate).ToList();   // 升序
        foreach (var bar in bars)
        {
            var i = UpperBound(dates, bar.TradeDate);                   // bisect_right
            if (i == 0) continue;                                       // 首事件前，因子 1
            var f = flag == "2" ? factors[i - 1].ForeAdjustFactor : factors[i - 1].BackAdjustFactor;
            bar.Open = Mul(bar.Open, f);
            bar.High = Mul(bar.High, f);
            bar.Low = Mul(bar.Low, f);
            bar.Close = Mul(bar.Close, f);
            bar.Preclose = Mul(bar.Preclose, f);
        }
    }

    private static decimal? Mul(decimal? v, decimal f) => v is decimal d ? d * f : null;

    // 首个 dates[i] > target 的下标（== bisect_right）
    private static int UpperBound(List<DateOnly> dates, DateOnly target)
    {
        int lo = 0, hi = dates.Count;
        while (lo < hi)
        {
            var mid = (lo + hi) / 2;
            if (dates[mid] <= target) lo = mid + 1; else hi = mid;
        }
        return lo;
    }
}

/// <summary>复权因子落盘（整段 upsert + 水位，同事务）。</summary>
public interface IAdjustFactorWriter
{
    Task<int> PersistAsync(string code, DateOnly end, FetchPayload payload, DateTimeOffset now, CancellationToken ct = default);
}

public sealed class AdjustFactorWriter(StockDataDbContext db) : IAdjustFactorWriter
{
    public async Task<int> PersistAsync(string code, DateOnly end, FetchPayload payload, DateTimeOffset now, CancellationToken ct = default)
    {
        var parsed = AdjustFactorParser.Parse(payload);
        var maxDate = parsed.Count == 0 ? (DateOnly?)null : parsed.Max(x => x.Date);
        var first = Coverage.BackfillStart("adjust_factor");           // 1990-12-19
        var last = Coverage.ClaimableLast("adjust_factor", end, maxDate, Coverage.Today(now));

        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var rows = parsed.Select(x => new object?[] { code, x.Date, x.Fore, x.Back, x.Adjust }).ToList();
        var n = rows.Count == 0 ? 0 : await SnapshotSql.UpsertAsync(db, "adjust_factor",
            new[] { "code", "divid_operate_date", "fore_adjust_factor", "back_adjust_factor", "adjust_factor" },
            new[] { "code", "divid_operate_date" }, rows, ct);

        // 空结果（从未除权）也推进水位，记录"已核实无因子"，防反复重抓
        await db.Database.ExecuteSqlRawAsync(
            """
            INSERT INTO data_watermark (code,data_type,first_date,last_date,last_fetched_at)
            VALUES (@code,'adjust_factor',@first,@last,now())
            ON CONFLICT (code,data_type) DO UPDATE SET
                first_date = LEAST(data_watermark.first_date, EXCLUDED.first_date),
                last_date  = GREATEST(data_watermark.last_date, EXCLUDED.last_date),
                last_fetched_at = now()
            """,
            new NpgsqlParameter("code", code), new NpgsqlParameter("first", first), new NpgsqlParameter("last", last));

        await tx.CommitAsync(ct);
        return n;
    }
}

/// <summary>
/// 复权因子编排。移植 fetch_adjust_factor：**恒从 A 股开市日整段抓取**（不取 coverage 的缺口区间，
/// 因 fore 序列随新除权全表重算），coverage 仅用于判断是否需要重抓。
/// </summary>
public sealed class AdjustFactorService(IWatermarkStore watermarks, IFetchClient fetch, IAdjustFactorWriter writer)
{
    public async Task EnsureFullAsync(string code, DateOnly end, DateTimeOffset now, CancellationToken ct = default)
    {
        var epoch = Coverage.BackfillStart("adjust_factor");
        var wm = await watermarks.GetAsync(code, "adjust_factor", ct);
        var decision = Coverage.CheckRange(wm?.ToWatermark(), "adjust_factor", epoch, end, now);
        if (decision.Fresh) return;

        var payload = await fetch.FetchAsync(
            new FetchRequest("fetch_adjust_factor", StartDate: epoch, EndDate: end, Code: code), ct);
        await writer.PersistAsync(code, end, payload, now, ct);
    }
}
