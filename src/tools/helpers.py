"""
辅助工具模块。

提供代码标准化和参数常量查询相关的 MCP 工具。
这些工具不依赖外部数据源，主要用于辅助 LLM 正确构造其他工具的输入参数。
"""
from typing import Optional

from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.core.helpers import normalize_index_code_logic, normalize_stock_code_logic

# 工具参数常量表：{ 类别名 → [(值, 含义), ...] }
_CONSTANTS = {
    "frequency": [
        ("d", "日线"), ("w", "周线"), ("m", "月线"),
        ("5", "5分钟"), ("15", "15分钟"), ("30", "30分钟"), ("60", "60分钟"),
    ],
    "adjust_flag": [
        ("1", "后复权"), ("2", "前复权"), ("3", "不复权"),
    ],
    "year_type": [
        ("report", "预案公告年份"), ("operate", "除权除息年份"),
    ],
    "index": [
        ("hs300", "沪深300"), ("sz50", "上证50"), ("zz500", "中证500"),
    ],
}


def _render_constants_md(kind: Optional[str]) -> str:
    """将常量表渲染为 Markdown 格式。"""
    key = (kind or "").strip().lower()
    targets = {key: _CONSTANTS[key]} if key and key in _CONSTANTS else _CONSTANTS
    if not targets:
        return f"错误: 无效的类别 '{kind}'。可选值: {', '.join(_CONSTANTS)}"
    sections = []
    for title, rows in targets.items():
        header = f"### {title}\n\n| 值 | 含义 |\n|---|---|\n"
        lines = [f"| {v} | {m} |" for v, m in rows]
        sections.append(header + "\n".join(lines))
    return "\n\n".join(sections)


@app.tool()
def normalize_stock_code(code: str) -> str:
    """将股票代码标准化为 Baostock 格式（如 'sh.600000'）。

    支持输入格式：sh.600000 / SH600000 / 600000.SH / 600000（纯数字自动推断交易所）
    """
    return run_tool_with_handling(
        lambda: normalize_stock_code_logic(code),
        context="normalize_stock_code",
    )


@app.tool()
def normalize_index_code(code: str) -> str:
    """将指数代码标准化为 Baostock 格式。

    支持输入：000300 / CSI300 / HS300（沪深300）、000016 / SSE50（上证50）、000905 / ZZ500（中证500）
    """
    return run_tool_with_handling(
        lambda: normalize_index_code_logic(code),
        context="normalize_index_code",
    )


@app.tool()
def list_tool_constants(kind: Optional[str] = None) -> str:
    """查询工具参数的合法常量值。

    Args:
        kind: 可选过滤（'frequency' / 'adjust_flag' / 'year_type' / 'index'），为 None 时显示全部
    """
    return _render_constants_md(kind)
