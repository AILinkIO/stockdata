namespace StockData.Mcp.Data.Entities;

/// <summary>
/// 数据水位（表 <c>data_watermark</c>，移植自 <c>db/models/meta.py</c>，设计文档 5.2.2）。
/// 每个 (code, data_type) 的覆盖区间与抓取时刻；全市场数据集 code 用空串。
///
/// 双水位：last_date 业务水位（覆盖到哪天）、last_fetched_at 系统水位（最后抓取时刻），
/// 新鲜度判断见 <see cref="Coverage"/>。覆盖范围是连续闭区间 [first_date, last_date]。
/// </summary>
public class DataWatermark
{
    public string Code { get; set; } = "";        // varchar(12)，全市场用 ""
    public string DataType { get; set; } = "";     // varchar(24)，对应 Python DataType

    public DateOnly? FirstDate { get; set; }
    public DateOnly LastDate { get; set; }
    public DateTimeOffset LastFetchedAt { get; set; }

    /// <summary>转为 Coverage 判定所需的纯输入（与 EF 实体解耦）。</summary>
    public Watermark ToWatermark() => new(FirstDate, LastDate, LastFetchedAt);
}
