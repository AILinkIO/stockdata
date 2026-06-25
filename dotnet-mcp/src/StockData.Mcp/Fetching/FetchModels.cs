namespace StockData.Mcp.Fetching;

/// <summary>
/// 抓取请求：Type = fetch_service 任务类型；其余字段按类型可选——
/// 区间类用 Start/End(+Code/Frequency)，快照类用 SnapDate(+Code/IndexCode)。
/// <see cref="ToParams"/> 折成 Python /fetch 的 params。
/// </summary>
public sealed record FetchRequest(
    string Type,
    DateOnly? StartDate = null, DateOnly? EndDate = null,
    string? Code = null, string? Frequency = null,
    DateOnly? SnapDate = null, string? IndexCode = null,
    string? Year = null, string? YearType = null,
    string? Kind = null, string? StartRaw = null, string? EndRaw = null,
    int? Quarter = null, string? ReportType = null)
{
    public IReadOnlyDictionary<string, string> ToParams()
    {
        var p = new Dictionary<string, string>();
        // start/end：宏观货币供应是 YYYY-MM/YYYY，用 StartRaw/EndRaw；其余用 ISO 日期
        if (StartRaw is not null) p["start_date"] = StartRaw;
        else if (StartDate is { } s) p["start_date"] = s.ToString("yyyy-MM-dd");
        if (EndRaw is not null) p["end_date"] = EndRaw;
        else if (EndDate is { } e) p["end_date"] = e.ToString("yyyy-MM-dd");
        if (SnapDate is { } sd) p["snap_date"] = sd.ToString("yyyy-MM-dd");
        if (Code is not null) p["code"] = Code;
        if (Frequency is not null) p["frequency"] = Frequency;
        if (IndexCode is not null) p["index_code"] = IndexCode;
        if (Year is not null) p["year"] = Year;
        if (YearType is not null) p["year_type"] = YearType;
        if (Kind is not null) p["kind"] = Kind;
        if (Quarter is { } q) p["quarter"] = q.ToString();
        if (ReportType is not null) p["report_type"] = ReportType;
        return p;
    }
}

/// <summary>baostock 原始返回（全字符串，dotnet 侧解析）。Fields = 列名，Rows = 行（与列同序）。</summary>
public sealed record FetchPayload(IReadOnlyList<string> Fields, IReadOnlyList<IReadOnlyList<string?>> Rows)
{
    public static readonly FetchPayload Empty = new(Array.Empty<string>(), Array.Empty<IReadOnlyList<string?>>());
}

/// <summary>job 状态机（对齐 Python /fetch 服务 Redis job）。</summary>
public enum FetchStatus { Pending, Running, Done, Failed }

/// <summary>POST /fetch 响应。</summary>
public sealed record FetchSubmitResponse(string JobId, FetchStatus Status, bool Dedup);

/// <summary>GET /fetch/{id} 响应。Payload 仅 Done 有；Error 仅 Failed 有。</summary>
public sealed record FetchJobResponse(string JobId, FetchStatus Status, FetchPayload? Payload, string? Error);
