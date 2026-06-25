namespace StockData.Mcp.Fetching;

/// <summary>抓取请求：给定区间的一段（已由 dotnet 切片，Python 不再切）。</summary>
public sealed record FetchRequest(string Code, DateOnly StartDate, DateOnly EndDate, string Frequency);

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
