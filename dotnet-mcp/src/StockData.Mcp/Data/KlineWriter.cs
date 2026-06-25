using System.Text;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using StockData.Mcp.Data.Entities;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>
/// 日线落盘（移植 <c>writer.py</c> 的 write_kline + update_watermark）。
/// 数据 upsert 与水位更新在**同一事务**：SIGKILL 时全提交/全回滚（复刻 SyncSession.begin）。
/// 批量 upsert 走原生 <c>INSERT … ON CONFLICT DO UPDATE</c>（TASK D-B），分块 1000 行/批。
/// </summary>
public sealed class KlineWriter(StockDataDbContext db) : IKlineWriter
{
    private const int Chunk = 1000;

    // 主键外的可更新列（updated_at 单独置 now()）
    private static readonly string[] DataCols =
    {
        "open", "high", "low", "close", "preclose", "volume", "amount", "turn", "pct_chg",
        "trade_status", "is_st", "pe_ttm", "pb_mrq", "ps_ttm", "pcf_ncf_ttm",
    };

    public async Task<int> PersistAsync(
        string code, string frequency, string dataType, FetchPayload payload,
        DateOnly sliceStart, DateOnly sliceEnd, DateTimeOffset now, CancellationToken ct = default)
    {
        var rows = KlineParser.ToKlines(payload, code, frequency);
        var actualLast = KlineParser.MaxDate(payload);
        var today = Coverage.Today(now);
        var lastDate = Coverage.ClaimableLast(dataType, sliceEnd, actualLast, today);

        await using var tx = await db.Database.BeginTransactionAsync(ct);

        var written = 0;
        for (var i = 0; i < rows.Count; i += Chunk)
        {
            var chunk = rows.GetRange(i, Math.Min(Chunk, rows.Count - i));
            written += await UpsertChunkAsync(chunk, ct);
        }

        await UpsertWatermarkAsync(code, dataType, sliceStart, lastDate, ct);
        await tx.CommitAsync(ct);
        return written;
    }

    private async Task<int> UpsertChunkAsync(List<Kline> chunk, CancellationToken ct)
    {
        var sql = new StringBuilder(
            "INSERT INTO kline (code,frequency,trade_date,open,high,low,close,preclose,volume,amount," +
            "turn,pct_chg,trade_status,is_st,pe_ttm,pb_mrq,ps_ttm,pcf_ncf_ttm,updated_at) VALUES ");
        var ps = new List<NpgsqlParameter>();
        var p = 0;

        for (var r = 0; r < chunk.Count; r++)
        {
            var k = chunk[r];
            if (r > 0) sql.Append(',');
            sql.Append('(')
                .Append($"@p{p++},@p{p++},@p{p++},@p{p++},@p{p++},@p{p++},@p{p++},@p{p++},@p{p++},@p{p++},")
                .Append($"@p{p++},@p{p++},@p{p++},@p{p++},@p{p++},@p{p++},@p{p++},@p{p++},now())");

            ps.Add(P(k.Code)); ps.Add(P(k.Frequency)); ps.Add(P(k.TradeDate));
            ps.Add(P(k.Open)); ps.Add(P(k.High)); ps.Add(P(k.Low)); ps.Add(P(k.Close)); ps.Add(P(k.Preclose));
            ps.Add(P(k.Volume)); ps.Add(P(k.Amount)); ps.Add(P(k.Turn)); ps.Add(P(k.PctChg));
            ps.Add(P(k.TradeStatus)); ps.Add(P(k.IsSt));
            ps.Add(P(k.PeTtm)); ps.Add(P(k.PbMrq)); ps.Add(P(k.PsTtm)); ps.Add(P(k.PcfNcfTtm));
        }

        sql.Append(" ON CONFLICT (code,frequency,trade_date) DO UPDATE SET ");
        for (var c = 0; c < DataCols.Length; c++)
        {
            if (c > 0) sql.Append(',');
            sql.Append(DataCols[c]).Append("=EXCLUDED.").Append(DataCols[c]);
        }
        sql.Append(",updated_at=now()");

        // 命名参数从 @p0 起，按位置绑定
        var idx = 0;
        foreach (var par in ps) par.ParameterName = $"p{idx++}";
        return await db.Database.ExecuteSqlRawAsync(sql.ToString(), ps, ct);
    }

    private Task UpsertWatermarkAsync(string code, string dataType, DateOnly firstDate, DateOnly lastDate, CancellationToken ct)
        => db.Database.ExecuteSqlRawAsync(
            """
            INSERT INTO data_watermark (code,data_type,first_date,last_date,last_fetched_at)
            VALUES (@code,@dt,@first,@last,now())
            ON CONFLICT (code,data_type) DO UPDATE SET
                first_date = LEAST(data_watermark.first_date, EXCLUDED.first_date),
                last_date  = GREATEST(data_watermark.last_date, EXCLUDED.last_date),
                last_fetched_at = now()
            """,
            new NpgsqlParameter("code", code),
            new NpgsqlParameter("dt", dataType),
            new NpgsqlParameter("first", firstDate),
            new NpgsqlParameter("last", lastDate));

    private static NpgsqlParameter P(object? value) => new() { Value = value ?? DBNull.Value };
}
