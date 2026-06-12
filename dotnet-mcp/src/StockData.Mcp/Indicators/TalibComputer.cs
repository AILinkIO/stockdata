using TALib;

namespace StockData.Mcp.Indicators;

/// <summary>
/// TALib 指标计算封装（TALib.NETCore 0.5.x Span API）。
///
/// API 约定：
///   - 输入：完整数组，Range = .. (Range.All)
///   - 输出：紧凑数组（packed），outRange.Start = 第一个有效值对应的输入索引，
///             outRange.End = 排他性结束（C# Range 语义）
///   - Align() 将紧凑输出还原为与输入等长的 nullable 数组，warmup 位置填 null
///
/// 已支持：
///   RSI  → MCP 工具 get_rsi
///   OBV  → MCP 工具 get_obv
///   EMA  → MCP 工具 get_vegas_channel / get_dual_ma
///   CCI  → 内部使用（不暴露 MCP），供后续指标服务层按需调用
/// </summary>
public static class TalibComputer
{
    // ── Lookback ─────────────────────────────────────────────────────

    public static int RsiLookback(int period) => Functions.RsiLookback(period);
    public static int ObvLookback()           => Functions.ObvLookback();
    public static int CciLookback(int period) => Functions.CciLookback(period);

    // ── EMA ──────────────────────────────────────────────────────────

    public static int EmaLookback(int period) => Functions.EmaLookback(period);

    public static double?[] Ema(double[] close, int period)
    {
        var out1 = new double[close.Length];
        var rc = Functions.Ema(close.AsSpan(), .., out1.AsSpan(), out var outRange, period);
        return Align(out1, outRange, close.Length, rc);
    }

    // ── RSI ──────────────────────────────────────────────────────────

    public static double?[] Rsi(double[] close, int period)
    {
        var out1 = new double[close.Length];
        var rc = Functions.Rsi(close.AsSpan(), .., out1.AsSpan(), out var outRange, period);
        return Align(out1, outRange, close.Length, rc);
    }

    // ── OBV ──────────────────────────────────────────────────────────

    public static double?[] Obv(double[] close, double[] volume)
    {
        var out1 = new double[close.Length];
        var rc = Functions.Obv(close.AsSpan(), volume.AsSpan(), .., out1.AsSpan(), out var outRange);
        return Align(out1, outRange, close.Length, rc);
    }

    // ── CCI（内部用）─────────────────────────────────────────────────

    /// <summary>
    /// Commodity Channel Index（Lambert 1980，0.015 × MAD）。
    ///
    /// 缓存策略（调用方负责）：
    ///   KlineSeries 已在 KlineLoader 层缓存（24h/5min TTL），CCI 计算本身 CPU 极轻，
    ///   通常无需单独缓存结果数组。若需跨请求复用，key 格式：
    ///     $"cci:{code}:{startDate}:{endDate}:{period}:{adjustFlag}"
    ///
    /// 周期建议（Lambert 原则）：period ≈ 主导周期 / 3；
    ///   默认 20（对应约 60 日主导周期），双周期系统可用 55/144（斐波那契近似）。
    /// </summary>
    public static double?[] Cci(double[] high, double[] low, double[] close, int period)
    {
        var out1 = new double[close.Length];
        var rc = Functions.Cci(high.AsSpan(), low.AsSpan(), close.AsSpan(), ..,
            out1.AsSpan(), out var outRange, period);
        return Align(out1, outRange, close.Length, rc);
    }

    // ── 内部工具 ─────────────────────────────────────────────────────

    /// <summary>
    /// 将 TALib 紧凑输出对齐为等长 nullable 数组。
    /// outRange.Start = packed[0] 对应的输入索引（warmup 结束位置）。
    /// outRange.End   = 排他性结束（C# Range 语义）。
    /// </summary>
    private static double?[] Align(
        double[] packed, Range outRange, int totalLen, Core.RetCode rc)
    {
        var result = new double?[totalLen];
        if (rc != Core.RetCode.Success) return result;
        var start = outRange.Start.Value;
        var count = outRange.End.Value - start;
        for (var i = 0; i < count; i++)
            result[start + i] = packed[i];
        return result;
    }
}
