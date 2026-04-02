"""
指数与行业分类工具模块。

提供 A 股市场指数成分股及行业分类相关的 MCP 工具，包括：
- 个股所属行业查询
- 上证50（sz50）、沪深300（hs300）、中证500（zz500）成分股查询
- 通用指数成分股查询
- 行业列表与行业成员查询
"""
import logging
from typing import Optional

from src.data_source import active_data_source
from src.formatting.markdown_formatter import format_table_output
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.services.validation import validate_index_key, validate_non_empty_str, validate_output_format

logger = logging.getLogger(__name__)

# 指数名称到内部 key 的映射，支持中英文别名
INDEX_MAP = {
    "hs300": "hs300", "沪深300": "hs300",
    "zz500": "zz500", "中证500": "zz500",
    "sz50": "sz50",   "上证50": "sz50",
}

# 内部 key 到数据源方法名的映射
_INDEX_METHOD = {"hs300": "get_hs300_stocks", "sz50": "get_sz50_stocks", "zz500": "get_zz500_stocks"}


def _fetch_index_constituents(*, index: str, date: Optional[str], limit: int, format: str) -> str:
    """根据指数 key 调用对应的数据源方法并格式化输出。"""
    validate_output_format(format)
    key = validate_index_key(index, INDEX_MAP)
    method = getattr(active_data_source, _INDEX_METHOD[key])
    df = method(date=date)
    meta = {"index": key, "as_of": date or "latest"}
    return format_table_output(df, format=format, max_rows=limit, meta=meta)


@app.tool()
def get_stock_industry(code: Optional[str] = None, date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """Get industry classification for a specific stock or all stocks on a date."""
    logger.info(f"Tool 'get_stock_industry' called for code={code or 'all'}, date={date or 'latest'}")

    def action():
        validate_output_format(format)
        df = active_data_source.get_stock_industry(code=code, date=date)
        meta = {"code": code or "all", "as_of": date or "latest"}
        return format_table_output(df, format=format, max_rows=limit, meta=meta)

    return run_tool_with_handling(action, context=f"get_stock_industry:{code or 'all'}")


@app.tool()
def get_sz50_stocks(date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """SZSE 50 constituents."""
    return run_tool_with_handling(
        lambda: _fetch_index_constituents(index="sz50", date=date, limit=limit, format=format),
        context="get_sz50_stocks",
    )


@app.tool()
def get_hs300_stocks(date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """CSI 300 constituents."""
    return run_tool_with_handling(
        lambda: _fetch_index_constituents(index="hs300", date=date, limit=limit, format=format),
        context="get_hs300_stocks",
    )


@app.tool()
def get_zz500_stocks(date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """CSI 500 constituents."""
    return run_tool_with_handling(
        lambda: _fetch_index_constituents(index="zz500", date=date, limit=limit, format=format),
        context="get_zz500_stocks",
    )


@app.tool()
def get_index_constituents(index: str, date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """Generic index constituent fetch (hs300/sz50/zz500)."""
    return run_tool_with_handling(
        lambda: _fetch_index_constituents(index=index, date=date, limit=limit, format=format),
        context=f"get_index_constituents:{index}",
    )


@app.tool()
def list_industries(date: Optional[str] = None, format: str = "markdown") -> str:
    """List distinct industries for a given date."""
    logger.info("Tool 'list_industries' called date=%s", date or "latest")

    def action():
        validate_output_format(format)
        df = active_data_source.get_stock_industry(code=None, date=date)
        if df is None or df.empty:
            return "(No data available to display)"
        col = "industry" if "industry" in df.columns else df.columns[-1]
        out = df[[col]].drop_duplicates().sort_values(by=col).rename(columns={col: "industry"})
        meta = {"as_of": date or "latest", "count": int(out.shape[0])}
        return format_table_output(out, format=format, max_rows=out.shape[0], meta=meta)

    return run_tool_with_handling(action, context="list_industries")


@app.tool()
def get_industry_members(industry: str, date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """Get all stocks in a given industry on a date."""
    logger.info("Tool 'get_industry_members' called industry=%s, date=%s", industry, date or "latest")

    def action():
        validate_output_format(format)
        validate_non_empty_str(industry, "industry")
        df = active_data_source.get_stock_industry(code=None, date=date)
        if df is None or df.empty:
            return "(No data available to display)"
        col = "industry" if "industry" in df.columns else df.columns[-1]
        filtered = df[df[col] == industry].copy()
        meta = {"industry": industry, "as_of": date or "latest"}
        return format_table_output(filtered, format=format, max_rows=limit, meta=meta)

    return run_tool_with_handling(action, context=f"get_industry_members:{industry}")
