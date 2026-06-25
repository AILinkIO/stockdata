using System.Globalization;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>分钟 K 线解析（移植 write_kline_minute）：bar_time 取 "time" 前 14 位(YYYYMMDDHHMMSS)+08。</summary>
public static class KlineMinuteParser
{
    public static DateTimeOffset? BarTime(string? s)
    {
        if (s is null || s.Length < 14) return null;
        return DateTime.TryParseExact(s[..14], "yyyyMMddHHmmss", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt)
            ? new DateTimeOffset(dt, TimeSpan.FromHours(8)) : null;
    }

    public static List<KlineMinute> Parse(FetchPayload payload, string code, short frequency)
    {
        var get = SnapshotSql.Accessor(payload);
        var rows = new List<KlineMinute>(payload.Rows.Count);
        foreach (var r in payload.Rows)
        {
            if (BarTime(get(r, "time")) is not DateTimeOffset bt) continue;
            rows.Add(new KlineMinute
            {
                Code = code, Frequency = frequency, BarTime = bt,
                Open = KlineParser.Dec(get(r, "open")), High = KlineParser.Dec(get(r, "high")),
                Low = KlineParser.Dec(get(r, "low")), Close = KlineParser.Dec(get(r, "close")),
                Volume = KlineParser.Int(get(r, "volume")), Amount = KlineParser.Dec(get(r, "amount")),
            });
        }
        return rows;
    }

    /// <summary>payload 中 bar_time 的最大业务日期（+08），供 ClaimableLast。</summary>
    public static DateOnly? MaxDate(List<KlineMinute> rows)
        => rows.Count == 0 ? null : DateOnly.FromDateTime(rows.Max(x => x.BarTime).DateTime);
}

public interface IKlineMinuteWriter
{
    Task<int> PersistAsync(string code, short frequency, string dataType, FetchPayload payload,
        DateOnly sliceStart, DateOnly sliceEnd, DateTimeOffset now, CancellationToken ct = default);
}

public sealed class KlineMinuteWriter(StockDataDbContext db) : IKlineMinuteWriter
{
    private const int Chunk = 1000;

    public async Task<int> PersistAsync(string code, short frequency, string dataType, FetchPayload payload,
        DateOnly sliceStart, DateOnly sliceEnd, DateTimeOffset now, CancellationToken ct = default)
    {
        var rows = KlineMinuteParser.Parse(payload, code, frequency);
        var last = Coverage.ClaimableLast(dataType, sliceEnd, KlineMinuteParser.MaxDate(rows), Coverage.Today(now));

        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var written = 0;
        for (var i = 0; i < rows.Count; i += Chunk)
            written += await UpsertChunkAsync(rows.GetRange(i, Math.Min(Chunk, rows.Count - i)), ct);

        await db.Database.ExecuteSqlRawAsync(
            """
            INSERT INTO data_watermark (code,data_type,first_date,last_date,last_fetched_at)
            VALUES (@code,@dt,@first,@last,now())
            ON CONFLICT (code,data_type) DO UPDATE SET
                first_date = LEAST(data_watermark.first_date, EXCLUDED.first_date),
                last_date  = GREATEST(data_watermark.last_date, EXCLUDED.last_date),
                last_fetched_at = now()
            """,
            new NpgsqlParameter("code", code), new NpgsqlParameter("dt", dataType),
            new NpgsqlParameter("first", sliceStart), new NpgsqlParameter("last", last));

        await tx.CommitAsync(ct);
        return written;
    }

    private Task<int> UpsertChunkAsync(List<KlineMinute> chunk, CancellationToken ct)
    {
        var sb = new StringBuilder(
            "INSERT INTO kline_minute (code,frequency,bar_time,open,high,low,close,volume,amount,updated_at) VALUES ");
        var ps = new List<NpgsqlParameter>();
        var p = 0;
        for (var r = 0; r < chunk.Count; r++)
        {
            var k = chunk[r];
            if (r > 0) sb.Append(',');
            sb.Append($"(@p{p},@p{p + 1},@p{p + 2},@p{p + 3},@p{p + 4},@p{p + 5},@p{p + 6},@p{p + 7},@p{p + 8},now())");
            // Npgsql：timestamptz 参数须为 UTC 偏移（同一时刻，PG 存 UTC）
            ps.Add(new($"p{p}", k.Code)); ps.Add(new($"p{p + 1}", k.Frequency)); ps.Add(new($"p{p + 2}", k.BarTime.ToUniversalTime()));
            ps.Add(new($"p{p + 3}", (object?)k.Open ?? DBNull.Value)); ps.Add(new($"p{p + 4}", (object?)k.High ?? DBNull.Value));
            ps.Add(new($"p{p + 5}", (object?)k.Low ?? DBNull.Value)); ps.Add(new($"p{p + 6}", (object?)k.Close ?? DBNull.Value));
            ps.Add(new($"p{p + 7}", (object?)k.Volume ?? DBNull.Value)); ps.Add(new($"p{p + 8}", (object?)k.Amount ?? DBNull.Value));
            p += 9;
        }
        sb.Append(" ON CONFLICT (code,frequency,bar_time) DO UPDATE SET ")
          .Append("open=EXCLUDED.open,high=EXCLUDED.high,low=EXCLUDED.low,close=EXCLUDED.close,")
          .Append("volume=EXCLUDED.volume,amount=EXCLUDED.amount,updated_at=now()");
        return db.Database.ExecuteSqlRawAsync(sb.ToString(), ps, ct);
    }
}

/// <summary>分钟 K 线编排（与 KlineService 同构，频率为 int，复用 fetch_kline，落 kline_minute）。</summary>
public sealed class KlineMinuteService(IWatermarkStore watermarks, IFetchClient fetch, IKlineMinuteWriter writer)
{
    public async Task EnsureRangeAsync(string code, short frequency, DateOnly start, DateOnly end, DateTimeOffset now, CancellationToken ct = default)
    {
        var dataType = $"k_{frequency}";
        var wm = await watermarks.GetAsync(code, dataType, ct);
        var decision = Coverage.CheckRange(wm?.ToWatermark(), dataType, start, end, now);
        if (decision.Fresh) return;

        var maxDays = RangeSlicer.SliceDays(dataType);
        foreach (var (fs, fe) in decision.FetchRanges)
        foreach (var (ss, se) in RangeSlicer.Slice(fs, fe, maxDays))
        {
            var payload = await fetch.FetchAsync(
                new FetchRequest("fetch_kline", StartDate: ss, EndDate: se, Code: code, Frequency: frequency.ToString()), ct);
            await writer.PersistAsync(code, frequency, dataType, payload, ss, se, now, ct);
        }
    }
}
