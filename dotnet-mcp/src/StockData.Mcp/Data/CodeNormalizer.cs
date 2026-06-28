using System.Text.RegularExpressions;

namespace StockData.Mcp.Data;

/// <summary>
/// 股票代码归一化为 baostock 形式（sh./sz./bj. 前缀）。
/// 支持工具文档声明的输入：sh.600000 / 600000 / 600000.SH 等。
/// </summary>
public static partial class CodeNormalizer
{
    [GeneratedRegex(@"^(sh|sz|bj)\.(\d{6})$", RegexOptions.IgnoreCase)]
    private static partial Regex Prefixed();

    [GeneratedRegex(@"^(\d{6})\.(sh|sz|bj)$", RegexOptions.IgnoreCase)]
    private static partial Regex Suffixed();

    [GeneratedRegex(@"^\d{6}$")]
    private static partial Regex BareDigits();

    /// <summary>
    /// 是否已处于 baostock 前缀形式（sh./sz./bj. + 6 位数字）。
    /// 接受大写前缀（内部 <see cref="ToBaostock"/> 仍会小写化输出）。
    /// 仅作「能不能直接用」的快速判定，不负责补全裸 6 位 → 自动推断交易所。
    /// </summary>
    public static bool IsValid(string? code) =>
        !string.IsNullOrWhiteSpace(code) && Prefixed().IsMatch(code.Trim());

    public static string ToBaostock(string code)
    {
        var c = code.Trim();

        var m = Prefixed().Match(c);
        if (m.Success) return $"{m.Groups[1].Value.ToLowerInvariant()}.{m.Groups[2].Value}";

        m = Suffixed().Match(c);
        if (m.Success) return $"{m.Groups[2].Value.ToLowerInvariant()}.{m.Groups[1].Value}";

        if (BareDigits().IsMatch(c)) return $"{InferMarket(c)}.{c}";

        return c;  // 未知形式原样下传（下游抓取失败即暴露，不静默猜测）
    }

    // 交易所推断：6/5/9→上交所，4/8→北交所，其余（0/2/3/1）→深交所
    private static string InferMarket(string digits) => digits[0] switch
    {
        '6' or '5' or '9' => "sh",
        '4' or '8' => "bj",
        _ => "sz",
    };
}
