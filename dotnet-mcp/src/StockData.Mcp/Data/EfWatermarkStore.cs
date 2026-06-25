using Microsoft.EntityFrameworkCore;
using StockData.Mcp.Data.Entities;

namespace StockData.Mcp.Data;

/// <summary>EF Core 水位读取（只读，AsNoTracking）。</summary>
public sealed class EfWatermarkStore(StockDataDbContext db) : IWatermarkStore
{
    public Task<DataWatermark?> GetAsync(string code, string dataType, CancellationToken ct = default)
        => db.DataWatermarks.AsNoTracking()
            .FirstOrDefaultAsync(w => w.Code == code && w.DataType == dataType, ct);
}
