"""
指数与行业分类工具模块。

提供 A 股市场指数成分股及行业分类相关的 MCP 工具，包括：
- 个股所属行业查询
- 上证50 / 沪深300 / 中证500 成分股查询
- 行业列表与行业成员查询
"""
from typing import Optional

from src.data_source import active_data_source
from src.formatting.markdown import format_table_output
from src.server import app
from src.services.tool_runner import run_tool_with_handling
from src.services.validation import validate_index_key, validate_non_empty_str, validate_output_format

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
    df = getattr(active_data_source, _INDEX_METHOD[key])(date=date)
    return format_table_output(df, format=format, max_rows=limit, meta={"index": key, "as_of": date or "最新"})


@app.tool()
def get_stock_industry(code: Optional[str] = None, date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """获取个股或全部股票的行业分类信息。

    Args:
        code: 股票代码，为 None 时返回全部股票的行业分类
        date: 查询日期 'YYYY-MM-DD'，为 None 时使用最新数据
        limit: 最大返回行数，默认 250
        format: 输出格式，默认 'markdown'
    """
    def action():
        validate_output_format(format)
        df = active_data_source.get_stock_industry(code=code, date=date)
        return format_table_output(df, format=format, max_rows=limit,
                                   meta={"code": code or "全部", "as_of": date or "最新"})

    return run_tool_with_handling(action, context=f"get_stock_industry:{code or 'all'}")


@app.tool()
def get_sz50_stocks(date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """获取上证50成分股列表。"""
    return run_tool_with_handling(
        lambda: _fetch_index_constituents(index="sz50", date=date, limit=limit, format=format),
        context="get_sz50_stocks",
    )


@app.tool()
def get_hs300_stocks(date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """获取沪深300成分股列表。"""
    return run_tool_with_handling(
        lambda: _fetch_index_constituents(index="hs300", date=date, limit=limit, format=format),
        context="get_hs300_stocks",
    )


@app.tool()
def get_zz500_stocks(date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """获取中证500成分股列表。"""
    return run_tool_with_handling(
        lambda: _fetch_index_constituents(index="zz500", date=date, limit=limit, format=format),
        context="get_zz500_stocks",
    )


@app.tool()
def get_index_constituents(index: str, date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """获取指定指数的成分股列表（支持 hs300 / sz50 / zz500）。"""
    return run_tool_with_handling(
        lambda: _fetch_index_constituents(index=index, date=date, limit=limit, format=format),
        context=f"get_index_constituents:{index}",
    )


@app.tool()
def list_industries(date: Optional[str] = None, format: str = "markdown") -> str:
    """获取所有行业分类名称列表。"""
    def action():
        validate_output_format(format)
        df = active_data_source.get_stock_industry(code=None, date=date)
        if df is None or df.empty:
            return "(无可用数据)"
        col = "industry" if "industry" in df.columns else df.columns[-1]
        out = df[[col]].drop_duplicates().sort_values(by=col).rename(columns={col: "industry"})
        return format_table_output(out, format=format, max_rows=out.shape[0],
                                   meta={"as_of": date or "最新", "count": int(out.shape[0])})

    return run_tool_with_handling(action, context="list_industries")


@app.tool()
def get_industry_members(industry: str, date: Optional[str] = None, limit: int = 250, format: str = "markdown") -> str:
    """获取指定行业下的所有股票。"""
    def action():
        validate_output_format(format)
        validate_non_empty_str(industry, "industry")
        df = active_data_source.get_stock_industry(code=None, date=date)
        if df is None or df.empty:
            return "(无可用数据)"
        col = "industry" if "industry" in df.columns else df.columns[-1]
        filtered = df[df[col] == industry]
        return format_table_output(filtered, format=format, max_rows=limit,
                                   meta={"industry": industry, "as_of": date or "最新"})

    return run_tool_with_handling(action, context=f"get_industry_members:{industry}")
