"""
MCP 薄壳：将旧 MCP 工具面 1:1 转发到 stockdata REST API（迁移计划阶段 7）。

纯 HTTP 转发，无业务逻辑（少数旧工具是对列表结果的简单过滤，在此就地实现）。
工具名与参数与 pre-restructure 版本保持兼容；返回 JSON 文本（原 markdown/csv
格式参数已移除）。

启动:
    uv run --group mcp python -m mcp_shim.main
    # MCP 端点: http://0.0.0.0:8001/mcp （streamable-http）

环境变量:
    STOCKDATA_API_BASE  REST API 地址（默认 http://127.0.0.1:8000）
    STOCKDATA_MCP_PORT  监听端口（默认 8001）
"""

import json
import os
from typing import Optional

import httpx
from fastmcp import FastMCP

API_BASE = os.environ.get("STOCKDATA_API_BASE", "http://127.0.0.1:8000")
MCP_PORT = int(os.environ.get("STOCKDATA_MCP_PORT", "8001"))

app = FastMCP("stockdata")
_client = httpx.Client(base_url=API_BASE, timeout=120)


def _get(path: str, limit: Optional[int] = None, **params) -> str:
    """转发 GET 请求；HTTP 错误转为 'Error: ...' 字符串（沿用旧 tool_runner 约定）。"""
    query = {k: v for k, v in params.items() if v is not None}
    try:
        r = _client.get(path, params=query)
    except httpx.HTTPError as e:
        return f"Error: 无法连接数据服务 {API_BASE}: {e}"
    if r.status_code >= 400:
        try:
            detail = r.json().get("detail", r.text)
        except Exception:
            detail = r.text
        return f"Error: {detail}"
    if "text/plain" in r.headers.get("content-type", ""):
        return r.text
    data = r.json()
    note = ""
    if limit is not None and isinstance(data, list) and len(data) > limit:
        note = f"\n（共 {len(data)} 行，已截断为前 {limit} 行）"
        data = data[:limit]
    return json.dumps(data, ensure_ascii=False, indent=1, default=str) + note


def _get_rows(path: str, **params) -> list | str:
    """取回 JSON 列表（供需要本地过滤的工具用）；失败返回错误字符串。"""
    query = {k: v for k, v in params.items() if v is not None}
    try:
        r = _client.get(path, params=query)
    except httpx.HTTPError as e:
        return f"Error: 无法连接数据服务 {API_BASE}: {e}"
    if r.status_code >= 400:
        try:
            return f"Error: {r.json().get('detail', r.text)}"
        except Exception:
            return f"Error: {r.text}"
    return r.json()


def _dump(rows: list, limit: int) -> str:
    note = f"\n（共 {len(rows)} 行，已截断为前 {limit} 行）" if len(rows) > limit else ""
    return json.dumps(rows[:limit], ensure_ascii=False, indent=1, default=str) + note


# ── 股票行情 ──


@app.tool()
def get_historical_k_data(
    code: str, start_date: str, end_date: str,
    frequency: str = "d", adjust_flag: str = "3", limit: int = 250,
) -> str:
    """获取股票历史K线数据。frequency: d日/w周/m月/5/15/30/60分钟；adjust_flag: 1后复权/2前复权/3不复权。"""
    if frequency in ("5", "15", "30", "60"):
        return _get(f"/api/v1/stocks/{code}/kline-minute", limit=limit,
                    start_date=start_date, end_date=end_date, frequency=frequency)
    return _get(f"/api/v1/stocks/{code}/kline", limit=limit,
                start_date=start_date, end_date=end_date,
                frequency=frequency, adjust_flag=adjust_flag)


@app.tool()
def get_stock_basic_info(code: str) -> str:
    """获取股票基本信息（名称、上市日期、类型、状态）。"""
    return _get(f"/api/v1/stocks/{code}/basic")


@app.tool()
def get_dividend_data(code: str, year: str, year_type: str = "report", limit: int = 250) -> str:
    """获取分红送转数据。year_type: report预案公告年份/operate除权除息年份。"""
    return _get(f"/api/v1/stocks/{code}/dividends", limit=limit,
                year=year, year_type=year_type)


@app.tool()
def get_adjust_factor_data(code: str, start_date: str, end_date: str, limit: int = 250) -> str:
    """获取复权因子数据（每个除权除息事件一行）。"""
    return _get(f"/api/v1/stocks/{code}/adjust-factors", limit=limit,
                start_date=start_date, end_date=end_date)


# ── 财务报表 ──


def _quarterly(report_type: str, code: str, year: str, quarter: int, limit: int) -> str:
    return _get(f"/api/v1/stocks/{code}/financials/{report_type}", limit=limit,
                year=year, quarter=quarter)


@app.tool()
def get_profit_data(code: str, year: str, quarter: int, limit: int = 250) -> str:
    """获取季度盈利能力数据（ROE、净利率、毛利率、EPS等）。"""
    return _quarterly("profit", code, year, quarter, limit)


@app.tool()
def get_operation_data(code: str, year: str, quarter: int, limit: int = 250) -> str:
    """获取季度营运能力数据（周转率/周转天数）。"""
    return _quarterly("operation", code, year, quarter, limit)


@app.tool()
def get_growth_data(code: str, year: str, quarter: int, limit: int = 250) -> str:
    """获取季度成长能力数据（同比增长率）。"""
    return _quarterly("growth", code, year, quarter, limit)


@app.tool()
def get_balance_data(code: str, year: str, quarter: int, limit: int = 250) -> str:
    """获取季度偿债能力数据（流动比率、资产负债率等）。"""
    return _quarterly("balance", code, year, quarter, limit)


@app.tool()
def get_cash_flow_data(code: str, year: str, quarter: int, limit: int = 250) -> str:
    """获取季度现金流量数据。"""
    return _quarterly("cash_flow", code, year, quarter, limit)


@app.tool()
def get_dupont_data(code: str, year: str, quarter: int, limit: int = 250) -> str:
    """获取季度杜邦分析数据。"""
    return _quarterly("dupont", code, year, quarter, limit)


@app.tool()
def get_performance_express_report(code: str, start_date: str, end_date: str, limit: int = 250) -> str:
    """获取业绩快报（按披露日期范围查询）。"""
    return _get(f"/api/v1/stocks/{code}/financials/express", limit=limit,
                start_date=start_date, end_date=end_date)


@app.tool()
def get_forecast_report(code: str, start_date: str, end_date: str, limit: int = 250) -> str:
    """获取业绩预告（按披露日期范围查询）。"""
    return _get(f"/api/v1/stocks/{code}/financials/forecast", limit=limit,
                start_date=start_date, end_date=end_date)


@app.tool()
def get_fina_indicator(code: str, start_date: str, end_date: str, limit: int = 250) -> str:
    """获取综合财务指标（六类季度财报按报告期合并，字段带类别前缀）。"""
    return _get(f"/api/v1/stocks/{code}/financials/indicator", limit=limit,
                start_date=start_date, end_date=end_date)


# ── 指数与行业 ──

_INDEX_ALIAS = {
    "sz50": "sz50", "sse50": "sz50", "000016": "sz50",
    "hs300": "hs300", "csi300": "hs300", "000300": "hs300",
    "zz500": "zz500", "csi500": "zz500", "000905": "zz500",
}


@app.tool()
def get_index_constituents(index: str, date: Optional[str] = None, limit: int = 600) -> str:
    """获取指数成分股。index: sz50/hs300/zz500（或 000016/000300/000905 等别名）。"""
    key = _INDEX_ALIAS.get(index.strip().lower())
    if not key:
        return f"Error: 不支持的指数 '{index}'，可选: sz50/hs300/zz500"
    return _get(f"/api/v1/indices/{key}/constituents", limit=limit, snap_date=date)


@app.tool()
def get_sz50_stocks(date: Optional[str] = None, limit: int = 250) -> str:
    """获取上证50成分股。"""
    return _get("/api/v1/indices/sz50/constituents", limit=limit, snap_date=date)


@app.tool()
def get_hs300_stocks(date: Optional[str] = None, limit: int = 350) -> str:
    """获取沪深300成分股。"""
    return _get("/api/v1/indices/hs300/constituents", limit=limit, snap_date=date)


@app.tool()
def get_zz500_stocks(date: Optional[str] = None, limit: int = 550) -> str:
    """获取中证500成分股。"""
    return _get("/api/v1/indices/zz500/constituents", limit=limit, snap_date=date)


@app.tool()
def get_stock_industry(code: Optional[str] = None, date: Optional[str] = None, limit: int = 250) -> str:
    """获取行业分类信息。code 为空时返回全部股票的行业分类。"""
    return _get("/api/v1/industries", limit=limit, code=code, snap_date=date)


@app.tool()
def list_industries(date: Optional[str] = None) -> str:
    """列出全部行业名称及成分股数量。"""
    rows = _get_rows("/api/v1/industries", snap_date=date)
    if isinstance(rows, str):
        return rows
    counts: dict[str, int] = {}
    for r in rows:
        counts[r.get("industry") or "未分类"] = counts.get(r.get("industry") or "未分类", 0) + 1
    return json.dumps(dict(sorted(counts.items())), ensure_ascii=False, indent=1)


@app.tool()
def get_industry_members(industry: str, date: Optional[str] = None, limit: int = 250) -> str:
    """获取某行业的全部成分股（行业名支持子串匹配）。"""
    rows = _get_rows("/api/v1/industries", snap_date=date)
    if isinstance(rows, str):
        return rows
    hit = [r for r in rows if industry in (r.get("industry") or "")]
    if not hit:
        return f"Error: 未找到行业 '{industry}'"
    return _dump(hit, limit)


# ── 市场概览 ──


@app.tool()
def get_trade_dates(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250) -> str:
    """获取交易日历（每天标注是否交易日）。"""
    from datetime import date as _date, timedelta

    end = end_date or _date.today().isoformat()
    start = start_date or (_date.fromisoformat(end) - timedelta(days=90)).isoformat()
    return _get("/api/v1/market/trade-calendar", limit=limit,
                start_date=start, end_date=end)


@app.tool()
def get_all_stock(date: Optional[str] = None, limit: int = 250) -> str:
    """获取全部股票（含指数）列表及交易状态。"""
    return _get("/api/v1/market/stocks", limit=limit, snap_date=date)


@app.tool()
def search_stocks(keyword: str, date: Optional[str] = None, limit: int = 50) -> str:
    """按代码或名称关键字搜索股票。"""
    rows = _get_rows("/api/v1/market/stocks", snap_date=date)
    if isinstance(rows, str):
        return rows
    kw = keyword.strip().lower()
    hit = [r for r in rows
           if kw in r["code"].lower() or kw in (r.get("code_name") or "").lower()]
    if not hit:
        return f"Error: 未找到匹配 '{keyword}' 的股票"
    return _dump(hit, limit)


@app.tool()
def get_suspensions(date: Optional[str] = None, limit: int = 250) -> str:
    """获取停牌股票列表。"""
    rows = _get_rows("/api/v1/market/stocks", snap_date=date)
    if isinstance(rows, str):
        return rows
    hit = [r for r in rows if r.get("trade_status") is False]
    return _dump(hit, limit)


# ── 宏观经济 ──


@app.tool()
def get_deposit_rate_data(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250) -> str:
    """获取基准存款利率（活期/定期各期限）。"""
    return _get("/api/v1/macro/deposit-rate", limit=limit,
                start_date=start_date, end_date=end_date)


@app.tool()
def get_loan_rate_data(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250) -> str:
    """获取基准贷款利率（各期限）。"""
    return _get("/api/v1/macro/loan-rate", limit=limit,
                start_date=start_date, end_date=end_date)


@app.tool()
def get_required_reserve_ratio_data(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250) -> str:
    """获取存款准备金率（大型/中小型机构）。"""
    return _get("/api/v1/macro/rrr", limit=limit,
                start_date=start_date, end_date=end_date)


@app.tool()
def get_money_supply_data_month(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 250) -> str:
    """获取月度货币供应量（M0/M1/M2 余额与同比环比）。日期格式 YYYY-MM-DD。"""
    return _get("/api/v1/macro/money-supply/month", limit=limit,
                start_date=start_date, end_date=end_date)


@app.tool()
def get_money_supply_data_year(start_year: Optional[int] = None, end_year: Optional[int] = None, limit: int = 250) -> str:
    """获取年度货币供应量（年末余额与同比）。"""
    return _get("/api/v1/macro/money-supply/year", limit=limit,
                start_year=start_year, end_year=end_year)


# ── 日期工具 ──


@app.tool()
def get_latest_trading_date() -> str:
    """获取最近的交易日（今天若是交易日则返回今天）。"""
    return _get("/api/v1/dates/latest-trading-day")


@app.tool()
def is_trading_day(date: str) -> str:
    """判断指定日期（YYYY-MM-DD）是否为交易日。"""
    return _get("/api/v1/dates/is-trading-day", date=date)


@app.tool()
def previous_trading_day(date: str) -> str:
    """获取指定日期之前最近的交易日。"""
    return _get("/api/v1/dates/previous-trading-day", date=date)


@app.tool()
def next_trading_day(date: str) -> str:
    """获取指定日期之后最近的交易日。"""
    return _get("/api/v1/dates/next-trading-day", date=date)


@app.tool()
def get_last_n_trading_days(days: int = 5) -> str:
    """获取最近 N 个交易日列表。"""
    return _get("/api/v1/dates/last-trading-days", days=days)


@app.tool()
def get_recent_trading_range(days: int = 5) -> str:
    """获取最近 N 个交易日的起止日期（适合作为 K 线查询范围）。"""
    data = _get_rows("/api/v1/dates/last-trading-days", days=days)
    if isinstance(data, str):
        return data
    dates = data.get("dates", [])
    if not dates:
        return "Error: 交易日历数据缺失"
    return json.dumps({"start_date": dates[0], "end_date": dates[-1]}, ensure_ascii=False)


# ── 分析与工具 ──


@app.tool()
def get_stock_analysis(code: str, analysis_type: str = "fundamental") -> str:
    """生成个股分析报告（Markdown）。analysis_type: fundamental基本面/technical技术面/comprehensive综合。"""
    return _get(f"/api/v1/stocks/{code}/analysis", analysis_type=analysis_type)


@app.tool()
def normalize_stock_code(code: str) -> str:
    """将任意常见格式的股票代码标准化为 Baostock 格式（如 sh.600000）。"""
    return _get("/api/v1/utils/normalize-code", code=code)


@app.tool()
def normalize_index_code(code: str) -> str:
    """将指数代码或别名标准化为 Baostock 格式（如 sh.000300）。"""
    return _get("/api/v1/utils/normalize-index-code", code=code)


if __name__ == "__main__":
    app.run(transport="streamable-http", host="0.0.0.0", port=MCP_PORT)
