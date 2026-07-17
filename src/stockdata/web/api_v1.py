"""/api/v1 只读数据面：供下游拉取数据的 RESTful API。

- 全部纯 PG 查询，绝不触碰 baostock（与 db/queries.py 同一原则）。
- 响应信封：{"data": ..., "meta": {...}}；错误走 FastAPI {"detail": ...}。
- 鉴权：settings.api_key 为空 = 关闭；配置后所有端点要求 X-API-Key 头。
- 复权（fore/back）由服务端读时计算，与 Web 图表同一套 back 因子推导口径。
- 批量端点用 POST + JSON body（codes ≤ 500），返回按 code 分组的字典。
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, Field

from stockdata.config import settings
from stockdata.db import queries

Freq = Literal["5", "30", "d", "w"]
Adjust = Literal["none", "fore", "back"]
FinType = Literal[
    "profit", "operation", "growth", "balance", "cash_flow", "dupont",
    "performance_express", "forecast",
]
MacroKind = Literal[
    "deposit_rate", "loan_rate", "rrr", "money_supply_month", "money_supply_year",
]
IndexCode = Literal["sz50", "hs300", "zz500"]

MAX_BATCH_CODES = 500


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if settings.api_key and x_api_key != settings.api_key:
        raise HTTPException(401, "无效或缺失的 API Key（X-API-Key 头）")


router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])


def _env(data, **meta) -> dict:
    return {"data": data, "meta": meta}


def _numify(rows: list[dict]) -> list[dict]:
    """Decimal → float，保证数值列 JSON 输出为 number 而非字符串。"""
    for r in rows:
        for k, v in r.items():
            if isinstance(v, Decimal):
                r[k] = float(v)
    return rows


# ── 复权（读时计算，back 因子 as-of 对齐；与 web/charts.py 同口径）──


def _apply_adjust(rows: list[dict], factors: list[dict], adjust: str,
                  minute: bool) -> None:
    if adjust == "none" or not factors or not rows:
        return
    fdates = [f["divid_operate_date"] for f in factors]
    fvals = [float(f["back_adjust_factor"] or 1.0) for f in factors]
    scale = 1.0 / fvals[-1] if adjust == "fore" else 1.0
    for r in rows:
        t = r["bar_time"] if minute else r["trade_date"]
        d = t.date() if isinstance(t, datetime) else t
        i = bisect_right(fdates, d) - 1
        b = (fvals[i] if i >= 0 else 1.0) * scale
        for k in ("open", "high", "low", "close"):
            if r[k] is not None:
                r[k] = round(float(r[k]) * b, 4)


# ── 行情 ──


@router.get("/kline/{code}", tags=["行情"])
def kline(
    code: str,
    freq: Freq = "d",
    start: date | None = None,
    end: date | None = None,
    adjust: Adjust = "none",
    limit: int = Query(10000, ge=1, le=100000),
) -> dict:
    rows = _numify(queries.kline_rows(code, freq, start, end, limit))
    _apply_adjust(rows, queries.adjust_factor_rows([code])[code], adjust,
                  minute=freq in ("5", "30"))
    return _env(rows, code=code, freq=freq, adjust=adjust,
                count=len(rows), truncated=len(rows) >= limit)


class KlineBatchRequest(BaseModel):
    codes: list[str] = Field(min_length=1, max_length=MAX_BATCH_CODES)
    freq: Freq = "d"
    start: date | None = None
    end: date | None = None
    adjust: Adjust = "none"
    limit_per_code: int = Field(5000, ge=1, le=20000)


@router.post("/kline/batch", tags=["行情"])
def kline_batch(req: KlineBatchRequest) -> dict:
    factors = queries.adjust_factor_rows(req.codes) if req.adjust != "none" else {}
    minute = req.freq in ("5", "30")
    data: dict[str, list] = {}
    truncated: list[str] = []
    for c in req.codes:
        rows = _numify(
            queries.kline_rows(c, req.freq, req.start, req.end, req.limit_per_code))
        _apply_adjust(rows, factors.get(c, []), req.adjust, minute)
        data[c] = rows
        if len(rows) >= req.limit_per_code:
            truncated.append(c)
    return _env(data, freq=req.freq, adjust=req.adjust, codes=len(req.codes),
                count=sum(len(v) for v in data.values()), truncated=truncated)


@router.get("/adjust-factors/{code}", tags=["行情"])
def adjust_factors(code: str) -> dict:
    rows = _numify(queries.adjust_factor_rows([code])[code])
    return _env(rows, code=code, count=len(rows))


class CodesRequest(BaseModel):
    codes: list[str] = Field(min_length=1, max_length=MAX_BATCH_CODES)


@router.post("/adjust-factors/batch", tags=["行情"])
def adjust_factors_batch(req: CodesRequest) -> dict:
    data = {c: _numify(rows) for c, rows in queries.adjust_factor_rows(req.codes).items()}
    return _env(data, codes=len(req.codes),
                count=sum(len(v) for v in data.values()))


# ── 参考数据 ──


@router.get("/securities", tags=["参考数据"])
def securities(
    type: int | None = None,  # noqa: A002  1 股票 2 指数 3 其他
    status: int | None = None,  # 1 上市 0 退市
    q: str = "",
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> dict:
    total, rows = queries.list_securities(type, status, q, limit, offset)
    return _env(rows, total=total, count=len(rows), limit=limit, offset=offset)


@router.get("/securities/{code}", tags=["参考数据"])
def security_detail(code: str) -> dict:
    info = queries.security_info(code)
    if info is None:
        raise HTTPException(404, f"证券不存在：{code}")
    return _env(info)


@router.get("/trade-calendar", tags=["参考数据"])
def trade_calendar(
    start: date | None = None,
    end: date | None = None,
    only_trading: bool = False,
) -> dict:
    rows = queries.trade_calendar_rows(start, end, only_trading)
    return _env(rows, count=len(rows), only_trading=only_trading)


@router.get("/industries", tags=["参考数据"])
def industries(date: date | None = None) -> dict:  # noqa: A002
    snap, rows = queries.industry_rows(date)
    return _env(rows, snap_date=snap, count=len(rows))


@router.get("/index-constituents/{index}", tags=["参考数据"])
def index_constituents(index: IndexCode, date: date | None = None) -> dict:  # noqa: A002
    snap, rows = queries.index_constituent_rows(index, date)
    return _env(rows, index=index, snap_date=snap, count=len(rows))


# ── 财务 / 事件 ──


@router.get("/financials/{code}", tags=["财务事件"])
def financials(
    code: str,
    type: FinType = "profit",  # noqa: A002
    start: date | None = None,
    end: date | None = None,
) -> dict:
    rows = queries.financial_rows([code], type, start, end)[code]
    return _env(rows, code=code, type=type, count=len(rows))


class FinancialsBatchRequest(BaseModel):
    codes: list[str] = Field(min_length=1, max_length=MAX_BATCH_CODES)
    type: FinType = "profit"
    start: date | None = None
    end: date | None = None


@router.post("/financials/batch", tags=["财务事件"])
def financials_batch(req: FinancialsBatchRequest) -> dict:
    data = queries.financial_rows(req.codes, req.type, req.start, req.end)
    return _env(data, type=req.type, codes=len(req.codes),
                count=sum(len(v) for v in data.values()))


@router.get("/dividends/{code}", tags=["财务事件"])
def dividends(code: str, year: int | None = None) -> dict:
    rows = queries.dividend_rows([code], year)[code]
    return _env(rows, code=code, year=year, count=len(rows))


class DividendsBatchRequest(BaseModel):
    codes: list[str] = Field(min_length=1, max_length=MAX_BATCH_CODES)
    year: int | None = None


@router.post("/dividends/batch", tags=["财务事件"])
def dividends_batch(req: DividendsBatchRequest) -> dict:
    data = queries.dividend_rows(req.codes, req.year)
    return _env(data, year=req.year, codes=len(req.codes),
                count=sum(len(v) for v in data.values()))


# ── 宏观 / 元数据 ──


@router.get("/macro/{kind}", tags=["宏观"])
def macro(kind: MacroKind, start: str | None = None, end: str | None = None) -> dict:
    rows = queries.macro_rows(kind, start, end)
    return _env(rows, kind=kind, count=len(rows))


@router.get("/meta/gaps", tags=["元数据"])
def kline_gaps(code: str, limit: int = Query(500, ge=1, le=10000)) -> dict:
    """日 K 缺口体检：水位区间内「是交易日但无行」的日期（停牌或真缺，需人工判断）。"""
    r = queries.kline_gaps(code)
    missing = r["missing"]
    return _env(missing[:limit], code=code,
                first_date=r["first_date"], last_date=r["last_date"],
                trading_days=r["trading_days"], missing_count=len(missing),
                truncated=len(missing) > limit)


@router.get("/meta/watermarks", tags=["元数据"])
def watermarks(
    code: str | None = None,
    dataset: str | None = None,
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
) -> dict:
    total, rows = queries.watermark_rows(code, dataset, limit, offset)
    return _env(rows, total=total, count=len(rows), limit=limit, offset=offset)
