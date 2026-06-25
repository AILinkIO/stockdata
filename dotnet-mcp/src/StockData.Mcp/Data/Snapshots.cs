using System.Text;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Data;

/// <summary>
/// 快照类数据集的抓取落盘单元（stock_basic / stock_list / industry / index_*）。
/// 各自封装：data_type、水位 code、has_rows 查询、fetch 参数、落盘+水位。
/// </summary>
public interface ISnapshotIngest
{
    string DataType { get; }
    string WatermarkCode { get; }
    Task<bool> HasRowsAsync(DateOnly snapDate, CancellationToken ct);
    FetchRequest BuildRequest(DateOnly snapDate);
    Task<int> PersistAsync(DateOnly snapDate, FetchPayload payload, CancellationToken ct);
}

/// <summary>
/// 快照读穿透编排（CheckSnapshot 样板，点状非区间）：has_rows + 水位 → 判定 → 抓取 → 落盘。
/// 历史快照永久有效；今日快照按刷新间隔节流。snap_date 由调用方解析（stock_basic=今天，
/// 其余=最近交易日）。
/// </summary>
public sealed class SnapshotService(IFetchClient fetch, IWatermarkStore watermarks)
{
    public async Task EnsureSnapshotAsync(ISnapshotIngest ingest, DateOnly snapDate, DateTimeOffset now, CancellationToken ct = default)
    {
        var hasRows = await ingest.HasRowsAsync(snapDate, ct);
        var wm = await watermarks.GetAsync(ingest.WatermarkCode, ingest.DataType, ct);
        var decision = Coverage.CheckSnapshot(wm?.ToWatermark(), ingest.DataType, snapDate, hasRows, now);
        if (decision.Fresh) return;

        var payload = await fetch.FetchAsync(ingest.BuildRequest(snapDate), ct);
        await ingest.PersistAsync(snapDate, payload, ct);
    }
}

/// <summary>快照落盘的共享 SQL（通用分块 upsert + 水位推进）。</summary>
internal static class SnapshotSql
{
    private const int Chunk = 1000;

    /// <summary>分块 INSERT … ON CONFLICT DO UPDATE。rows 与 cols 等长；updated_at 自动置 now()。</summary>
    public static async Task<int> UpsertAsync(
        StockDataDbContext db, string table, string[] cols, string[] pk, List<object?[]> rows, CancellationToken ct)
    {
        var total = 0;
        for (var off = 0; off < rows.Count; off += Chunk)
        {
            var slice = rows.GetRange(off, Math.Min(Chunk, rows.Count - off));
            var sb = new StringBuilder($"INSERT INTO {table} (")
                .Append(string.Join(",", cols)).Append(",updated_at) VALUES ");
            var ps = new List<NpgsqlParameter>();
            var pi = 0;
            for (var r = 0; r < slice.Count; r++)
            {
                if (r > 0) sb.Append(',');
                sb.Append('(');
                for (var c = 0; c < cols.Length; c++)
                {
                    if (c > 0) sb.Append(',');
                    sb.Append("@p").Append(pi);
                    ps.Add(new NpgsqlParameter($"p{pi}", slice[r][c] ?? DBNull.Value));
                    pi++;
                }
                sb.Append(",now())");
            }
            var setCols = cols.Where(c => !pk.Contains(c)).Select(c => $"{c}=EXCLUDED.{c}").Append("updated_at=now()");
            sb.Append($" ON CONFLICT ({string.Join(",", pk)}) DO UPDATE SET ").Append(string.Join(",", setCols));
            total += await db.Database.ExecuteSqlRawAsync(sb.ToString(), ps, ct);
        }
        return total;
    }

    /// <summary>快照水位推进：last_date = snapDate（first_date 不动）。</summary>
    public static Task WatermarkAsync(StockDataDbContext db, string code, string dataType, DateOnly last, CancellationToken ct)
        => db.Database.ExecuteSqlRawAsync(
            """
            INSERT INTO data_watermark (code,data_type,last_date,last_fetched_at)
            VALUES (@c,@d,@l,now())
            ON CONFLICT (code,data_type) DO UPDATE SET
                last_date = GREATEST(data_watermark.last_date, EXCLUDED.last_date),
                last_fetched_at = now()
            """,
            new NpgsqlParameter("c", code), new NpgsqlParameter("d", dataType), new NpgsqlParameter("l", last));

    public static string? NullIfEmpty(string? s) => string.IsNullOrEmpty(s) ? null : s;

    /// <summary>payload → 按列名取值的访问器。</summary>
    public static Func<IReadOnlyList<string?>, string, string?> Accessor(FetchPayload p)
    {
        var idx = new Dictionary<string, int>();
        for (var i = 0; i < p.Fields.Count; i++) idx[p.Fields[i]] = i;
        return (row, field) => idx.TryGetValue(field, out var i) && i < row.Count ? row[i] : null;
    }
}

/// <summary>股票基本信息（per-code，snap_date=今天）。移植 fetch_stock_basic + write_stock_basic。</summary>
public sealed class StockBasicIngest(StockDataDbContext db, string code) : ISnapshotIngest
{
    public string DataType => "stock_basic";
    public string WatermarkCode => code;

    public Task<bool> HasRowsAsync(DateOnly snapDate, CancellationToken ct)
        => db.StockBasics.AsNoTracking().AnyAsync(x => x.Code == code, ct);

    public FetchRequest BuildRequest(DateOnly snapDate) => new("fetch_stock_basic", Code: code);

    public async Task<int> PersistAsync(DateOnly snapDate, FetchPayload payload, CancellationToken ct)
    {
        var get = SnapshotSql.Accessor(payload);
        var rows = payload.Rows.Select(r => new object?[]
        {
            get(r, "code"), SnapshotSql.NullIfEmpty(get(r, "code_name")),
            KlineParser.Date(get(r, "ipoDate")), KlineParser.Date(get(r, "outDate")),
            KlineParser.Short(get(r, "type")), KlineParser.Short(get(r, "status")),
        }).Where(v => v[0] is string).ToList();

        await using var tx = await db.Database.BeginTransactionAsync(ct);
        var n = await SnapshotSql.UpsertAsync(db, "stock_basic",
            new[] { "code", "code_name", "ipo_date", "out_date", "type", "status" }, new[] { "code" }, rows, ct);
        await SnapshotSql.WatermarkAsync(db, code, DataType, snapDate, ct);
        await tx.CommitAsync(ct);
        return n;
    }
}

/// <summary>股票列表快照。移植 fetch_stock_list：空结果（盘中未发布）不写水位。</summary>
public sealed class StockListIngest(StockDataDbContext db) : ISnapshotIngest
{
    public string DataType => "stock_list";
    public string WatermarkCode => "";

    public Task<bool> HasRowsAsync(DateOnly snapDate, CancellationToken ct)
        => db.StockListSnapshots.AsNoTracking().AnyAsync(x => x.SnapDate == snapDate, ct);

    public FetchRequest BuildRequest(DateOnly snapDate) => new("fetch_stock_list", SnapDate: snapDate);

    public async Task<int> PersistAsync(DateOnly snapDate, FetchPayload payload, CancellationToken ct)
    {
        var get = SnapshotSql.Accessor(payload);
        var rows = payload.Rows.Select(r => new object?[]
        {
            snapDate, get(r, "code"), SnapshotSql.NullIfEmpty(get(r, "code_name")),
            get(r, "tradeStatus") is { } ts ? ts == "1" : (bool?)null,
        }).Where(v => v[1] is string).ToList();

        if (rows.Count == 0) return 0;   // 盘中未发布：不写水位，留待回退前一交易日

        await using var tx = await db.Database.BeginTransactionAsync(ct);
        var n = await SnapshotSql.UpsertAsync(db, "stock_list_snapshot",
            new[] { "snap_date", "code", "code_name", "trade_status" }, new[] { "snap_date", "code" }, rows, ct);
        await SnapshotSql.WatermarkAsync(db, "", DataType, snapDate, ct);
        await tx.CommitAsync(ct);
        return n;
    }
}

/// <summary>行业分类快照。移植 fetch_industry + write_industry。</summary>
public sealed class IndustryIngest(StockDataDbContext db) : ISnapshotIngest
{
    public string DataType => "industry";
    public string WatermarkCode => "";

    public Task<bool> HasRowsAsync(DateOnly snapDate, CancellationToken ct)
        => db.StockIndustries.AsNoTracking().AnyAsync(x => x.SnapDate == snapDate, ct);

    public FetchRequest BuildRequest(DateOnly snapDate) => new("fetch_industry", SnapDate: snapDate);

    public async Task<int> PersistAsync(DateOnly snapDate, FetchPayload payload, CancellationToken ct)
    {
        var get = SnapshotSql.Accessor(payload);
        var rows = payload.Rows.Select(r => new object?[]
        {
            snapDate, get(r, "code"), SnapshotSql.NullIfEmpty(get(r, "code_name")),
            SnapshotSql.NullIfEmpty(get(r, "industry")), SnapshotSql.NullIfEmpty(get(r, "industryClassification")),
        }).Where(v => v[1] is string).ToList();

        await using var tx = await db.Database.BeginTransactionAsync(ct);
        var n = await SnapshotSql.UpsertAsync(db, "stock_industry",
            new[] { "snap_date", "code", "code_name", "industry", "industry_classification" },
            new[] { "snap_date", "code" }, rows, ct);
        await SnapshotSql.WatermarkAsync(db, "", DataType, snapDate, ct);
        await tx.CommitAsync(ct);
        return n;
    }
}

/// <summary>指数成分股（sz50/hs300/zz500）。水位 data_type = index_{indexCode}。</summary>
public sealed class IndexConstituentIngest(StockDataDbContext db, string indexCode) : ISnapshotIngest
{
    public string DataType => $"index_{indexCode}";
    public string WatermarkCode => "";

    public Task<bool> HasRowsAsync(DateOnly snapDate, CancellationToken ct)
        => db.IndexConstituents.AsNoTracking().AnyAsync(x => x.IndexCode == indexCode && x.SnapDate == snapDate, ct);

    public FetchRequest BuildRequest(DateOnly snapDate) => new("fetch_index_constituent", IndexCode: indexCode, SnapDate: snapDate);

    public async Task<int> PersistAsync(DateOnly snapDate, FetchPayload payload, CancellationToken ct)
    {
        var get = SnapshotSql.Accessor(payload);
        var rows = payload.Rows.Select(r => new object?[]
        {
            indexCode, snapDate, get(r, "code"), SnapshotSql.NullIfEmpty(get(r, "code_name")),
        }).Where(v => v[2] is string).ToList();

        await using var tx = await db.Database.BeginTransactionAsync(ct);
        var n = await SnapshotSql.UpsertAsync(db, "index_constituent",
            new[] { "index_code", "snap_date", "code", "code_name" },
            new[] { "index_code", "snap_date", "code" }, rows, ct);
        await SnapshotSql.WatermarkAsync(db, "", DataType, snapDate, ct);
        await tx.CommitAsync(ct);
        return n;
    }
}
