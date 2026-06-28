using StockData.SyncCli.Progress;

namespace StockData.SyncCli.Ui;

/// <summary>
/// L2 步级状态渲染：把当前票的 <see cref="SyncProgress.CurrentStepsDone"/> 与固定步名序列
/// 对照，输出 done/running/pending 三态的紧凑字符串，供 <see cref="DashboardWindow"/>
/// 显示在「current task」面板。
///
/// 设计成纯静态、无 UI 依赖 — Phase 4 单测可对 <see cref="Render"/> 做断言，不需起
/// Terminal.Gui 驱动器。
///
/// 步名序列约定（与 StockSyncService 全量同步顺序对齐）：
/// - <c>full</c>：stock_basic → k_d → adjust_factor → dividend → financial → performance
/// - <c>minute</c>：k_5 → k_15 → k_30 → k_60
/// </summary>
public static class StepStatusView
{
    private const string DoneGlyph = "\u2713";   // ✓
    private const string RunGlyph = "\u25B6";   // ▶
    private const string PendingGlyph = "\u00B7"; // ·

    private static readonly IReadOnlyList<string> FullSteps = new[]
    {
        "stock_basic", "k_d", "adjust_factor", "dividend", "financial", "performance",
    };

    private static readonly IReadOnlyList<string> MinuteSteps = new[]
    {
        "k_5", "k_15", "k_30", "k_60",
    };

    /// <summary>
    /// 取得当前 kind 对应的固定步序列（不可变）。Unknown kind 退回 full。
    /// </summary>
    public static IReadOnlyList<string> StepsFor(string? kind) =>
        kind switch
        {
            "minute" => MinuteSteps,
            _ => FullSteps,
        };

    /// <summary>
    /// 渲染步级状态行，例如：
    /// <c>[stock_basic ✓] [k_d ✓] [dividend ▶] [financial ·] [performance ·]</c>
    ///
    /// 状态判定：
    /// - done：step 在 <paramref name="done"/> 中；
    /// - running：未 done、但 done 数 &gt; 0 且是该位置的下一个未完成 step；
    /// - pending：其余。
    /// </summary>
    public static string Render(IReadOnlyList<string>? done, IReadOnlyList<string> steps)
    {
        ArgumentNullException.ThrowIfNull(steps);

        var doneSet = done is null
            ? new HashSet<string>(StringComparer.Ordinal)
            : new HashSet<string>(done, StringComparer.Ordinal);

        // 第一个未完成 step 视为 running；其后皆 pending
        var runningMarked = false;
        var parts = new List<string>(steps.Count);
        foreach (var step in steps)
        {
            string glyph;
            if (doneSet.Contains(step))
            {
                glyph = DoneGlyph;
            }
            else if (!runningMarked && doneSet.Count > 0)
            {
                glyph = RunGlyph;
                runningMarked = true;
            }
            else
            {
                glyph = PendingGlyph;
            }
            parts.Add($"[{step} {glyph}]");
        }
        return string.Join(" ", parts);
    }

    /// <summary>便捷入口：按 kind 自动选序列再 render。</summary>
    public static string RenderForKind(IReadOnlyList<string>? done, string? kind) =>
        Render(done, StepsFor(kind));
}
