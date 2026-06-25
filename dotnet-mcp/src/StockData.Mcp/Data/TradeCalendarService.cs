using System.Text;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>交易日历落盘（ON CONFLICT upsert + 水位，同事务）。</summary>
public interface ITradeCalendarWriter
{
    Task<int> PersistAsync(FetchPayload payload, DateOnly sliceStart, DateOnly sliceEnd, CancellationToken ct = default);
}

/// <summary>
/// 交易日历落盘。移植自 fetcher.fetch_trade_calendar：
/// **水位 last_date = 区间末（不用 claimable_last——日历未来日有效、不节流）**，code=""（全市场）。
/// </summary>
public sealed class TradeCalendarWriter(StockDataDbContext db) : ITradeCalendarWriter
{
    private const int Chunk = 1000;

    public async Task<int> PersistAsync(FetchPayload payload, DateOnly sliceStart, DateOnly sliceEnd, CancellationToken ct = default)
    {
        var rows = TradeCalendarParser.Parse(payload);
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var written = 0;
        for (var i = 0; i < rows.Count; i += Chunk)
        {
            var chunk = rows.GetRange(i, Math.Min(Chunk, rows.Count - i));
            written += await UpsertChunkAsync(chunk, ct);
        }

        await db.Database.ExecuteSqlRawAsync(
            """
            INSERT INTO data_watermark (code,data_type,first_date,last_date,last_fetched_at)
            VALUES ('','trade_calendar',@first,@last,now())
            ON CONFLICT (code,data_type) DO UPDATE SET
                first_date = LEAST(data_watermark.first_date, EXCLUDED.first_date),
                last_date  = GREATEST(data_watermark.last_date, EXCLUDED.last_date),
                last_fetched_at = now()
            """,
            new NpgsqlParameter("first", sliceStart), new NpgsqlParameter("last", sliceEnd));

        await tx.CommitAsync(ct);
        return written;
    }

    private Task<int> UpsertChunkAsync(List<(DateOnly Date, bool IsTradingDay)> chunk, CancellationToken ct)
    {
        var sql = new StringBuilder("INSERT INTO trade_calendar (calendar_date,is_trading_day,updated_at) VALUES ");
        var ps = new List<NpgsqlParameter>();
        for (var r = 0; r < chunk.Count; r++)
        {
            if (r > 0) sql.Append(',');
            sql.Append($"(@d{r},@t{r},now())");
            ps.Add(new NpgsqlParameter($"d{r}", chunk[r].Date));
            ps.Add(new NpgsqlParameter($"t{r}", chunk[r].IsTradingDay));
        }
        sql.Append(" ON CONFLICT (calendar_date) DO UPDATE SET is_trading_day=EXCLUDED.is_trading_day, updated_at=now()");
        return db.Database.ExecuteSqlRawAsync(sql.ToString(), ps, ct);
    }
}

/// <summary>
/// 交易日历读穿透编排（非 k_d 类型样板）：coverage → 抓取 → 落盘。
/// 全市场数据集 code=""；不切片（RangeSlicer 不含 trade_calendar）；未来日期不钳制（coverage 已处理）。
/// </summary>
public sealed class TradeCalendarService(IWatermarkStore watermarks, IFetchClient fetch, ITradeCalendarWriter writer)
{
    public async Task EnsureRangeAsync(DateOnly start, DateOnly end, DateTimeOffset now, CancellationToken ct = default)
    {
        var wm = await watermarks.GetAsync("", "trade_calendar", ct);
        var decision = Coverage.CheckRange(wm?.ToWatermark(), "trade_calendar", start, end, now);
        if (decision.Fresh) return;

        foreach (var (fs, fe) in decision.FetchRanges)
        foreach (var (ss, se) in RangeSlicer.Slice(fs, fe, RangeSlicer.SliceDays("trade_calendar")))
        {
            var payload = await fetch.FetchAsync(
                new FetchRequest("fetch_trade_calendar", StartDate: ss, EndDate: se), ct);
            await writer.PersistAsync(payload, ss, se, ct);
        }
    }
}
