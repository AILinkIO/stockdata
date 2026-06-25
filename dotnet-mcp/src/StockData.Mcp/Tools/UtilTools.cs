using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using StockData.Mcp.Data;

namespace StockData.Mcp.Tools;

/// <summary>代码标准化（纯 dotnet 计算，不依赖后端）。</summary>
[McpServerToolType]
public static class UtilTools
{
    private static readonly Dictionary<string, string> IndexMap = new(StringComparer.OrdinalIgnoreCase)
    {
        ["sz50"] = "sh.000016", ["sse50"] = "sh.000016", ["000016"] = "sh.000016", ["上证50"] = "sh.000016",
        ["hs300"] = "sh.000300", ["csi300"] = "sh.000300", ["000300"] = "sh.000300", ["沪深300"] = "sh.000300",
        ["zz500"] = "sh.000905", ["csi500"] = "sh.000905", ["000905"] = "sh.000905", ["中证500"] = "sh.000905",
    };

    [McpServerTool(Name = "normalize_stock_code")]
    [Description("将任意常见格式的股票代码标准化为 Baostock 格式（如 600000.SH → sh.600000）。")]
    public static string NormalizeStockCode([Description("任意格式的股票代码")] string code)
        => JsonSerializer.Serialize(new { input = code, normalized = CodeNormalizer.ToBaostock(code) });

    [McpServerTool(Name = "normalize_index_code")]
    [Description("将指数代码或英文别名标准化为 Baostock 格式（CSI300/HS300/000300 → sh.000300）。")]
    public static string NormalizeIndexCode(string code)
        => IndexMap.TryGetValue(code.Trim(), out var v)
            ? JsonSerializer.Serialize(new { input = code, normalized = v })
            : $"Error: 无法识别的指数代码 '{code}'，支持 sz50/hs300/zz500 及别名";
}
