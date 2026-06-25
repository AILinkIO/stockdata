using System.Text.Json;
using StockData.Mcp.Fetching;

namespace StockData.Mcp.Tests;

/// <summary>
/// Python /fetch 返回的 snake_case JSON 与 dotnet DTO 的契约对齐
/// （job_id/fields/rows/status 枚举）。用 HttpFetchClient 实际使用的 Json 选项反序列化。
/// </summary>
public class FetchContractTests
{
    private static readonly JsonSerializerOptions Json = HttpFetchClient.Json;

    [Fact]
    public void POST_fetch_响应映射()
    {
        var dto = JsonSerializer.Deserialize<FetchSubmitResponse>(
            """{"job_id":"abc123","status":"pending","dedup":true}""", Json)!;
        Assert.Equal("abc123", dto.JobId);
        Assert.Equal(FetchStatus.Pending, dto.Status);
        Assert.True(dto.Dedup);
    }

    [Fact]
    public void GET_fetch_done_带payload映射()
    {
        var json = """
        {"job_id":"abc123","status":"done","error":null,
         "payload":{"fields":["date","close"],
                    "rows":[["2024-01-02","10.4"],["2024-01-03","10.5"]]}}
        """;
        var dto = JsonSerializer.Deserialize<FetchJobResponse>(json, Json)!;

        Assert.Equal("abc123", dto.JobId);
        Assert.Equal(FetchStatus.Done, dto.Status);
        Assert.NotNull(dto.Payload);
        Assert.Equal(new[] { "date", "close" }, dto.Payload!.Fields);
        Assert.Equal(2, dto.Payload.Rows.Count);
        Assert.Equal("10.5", dto.Payload.Rows[1][1]);
    }

    [Fact]
    public void GET_fetch_failed_带error()
    {
        var dto = JsonSerializer.Deserialize<FetchJobResponse>(
            """{"job_id":"x","status":"failed","error":"Baostock 登录失败","payload":null}""", Json)!;
        Assert.Equal(FetchStatus.Failed, dto.Status);
        Assert.Equal("Baostock 登录失败", dto.Error);
        Assert.Null(dto.Payload);
    }

    [Fact]
    public void 空串单元格保留_供解析器转null()
    {
        var dto = JsonSerializer.Deserialize<FetchJobResponse>(
            """{"job_id":"x","status":"done","payload":{"fields":["date","turn"],"rows":[["2024-01-02",""]]}}""",
            Json)!;
        Assert.Equal("", dto.Payload!.Rows[0][1]);
    }
}
