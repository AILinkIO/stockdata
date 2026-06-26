using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;

namespace StockData.Mcp.Data.Entities;

/// <summary>
/// 已纳管股票注册表（表 <c>synced_stock</c>）。懒加载：被查询过的 code 才登记，
/// 同步只覆盖此表里的票（TASK 本轮决策 ②⑤）。
/// </summary>
[PrimaryKey(nameof(Code))]
public class SyncedStock
{
    [MaxLength(12)] public string Code { get; set; } = "";
    /// <summary>首次被查询/登记的时刻。</summary>
    public DateTimeOffset FirstSeenAt { get; set; }
    /// <summary>是否也纳管分钟线（仅显式分钟线任务后置 true，默认否——分钟线不进默认同步）。</summary>
    public bool MinuteEnabled { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}

/// <summary>
/// 单票同步任务 + 状态（表 <c>stock_sync_task</c>）。一票一 kind 一行。
/// 断点续传：<see cref="DatasetsDone"/> 作粗粒度快跳，真正持久断点是 data_watermark
/// （Coverage 据其判 Fresh 跳过已完成数据集）。状态机：pending→running→done/partial/failed。
/// </summary>
[PrimaryKey(nameof(Code), nameof(Kind))]
public class StockSyncTask
{
    [MaxLength(12)] public string Code { get; set; } = "";
    /// <summary><c>full</c>（默认全数据集）/ <c>minute</c>（分钟线全历史，显式下达）。</summary>
    [MaxLength(8)] public string Kind { get; set; } = "full";
    /// <summary>pending / running / partial（中断待续）/ done / failed。</summary>
    [MaxLength(12)] public string Status { get; set; } = "pending";
    /// <summary>已完成的数据集名（如 stock_basic/k_d/adjust_factor…），续传快跳用。映射 PG text[]。</summary>
    public string[] DatasetsDone { get; set; } = [];
    public DateTimeOffset RequestedAt { get; set; }
    public DateTimeOffset? StartedAt { get; set; }
    public DateTimeOffset? FinishedAt { get; set; }
    [MaxLength(512)] public string? Error { get; set; }
    public int Attempt { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}
