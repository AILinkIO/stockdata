"""
AKShare 查询函数（baostock 的替代实现）。

设计：暴露与 fetcher/providers/baostock.py **完全同名**的 query_* 函数，且返回的
DataFrame **沿用 baostock 的列名 schema**，使 fetcher/writer.py 与数据模型零改动即可切换。

与 baostock 的关键差异及处置：
- 无账号/登录：akshare 爬取东财/新浪公开接口，不存在 baostock 的 IP 黑名单问题；
  但单接口偶发连接中断，统一经 _call 退避重试，仍失败抛 DataSourceError 交任务层重试。
- 成交量单位：东财为「手」，baostock 为「股」，统一 ×100 转股并取整。
- preclose：东财不直接给，按 收盘 − 涨跌额 派生。
- 复权：东财 raw 与 hfq **同源**，back=hfq收盘/raw收盘，fore=back/back_最新（前复权=后复权/最新后复权因子）。
  自洽地复现读时 raw×factor 口径，避免跨源（新浪/东财）复权约定不一致。
- 估值字段（peTTM/pbMRQ/psTTM/pcfNcfTTM）：东财日线不提供，缺省（writer 跳过缺列）。
  指标计算只用 OHLCV，不受影响；仅原始 K 线透传少这几列。

降级说明（见各函数 docstring 的 [降级]）：分钟线 / 个股基本信息 / 全市场列表 / 行业分类
从本机网络偶发不稳，存款利率/贷款利率（已被 LPR 取代的基准利率）无对应数据源。
"""

import logging
import os
import sys
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime

import akshare as ak
import pandas as pd

from core.timeutil import today_cst

from .interface import DataSourceError, NoDataFoundError

logger = logging.getLogger(__name__)

# akshare 底层用 requests，整体线程安全性不保证；与 baostock 一致地串行化，
# 同时充当对数据源的天然限流，降低被东财/新浪限流的概率。
_AK_LOCK = threading.RLock()
_MAX_RETRIES = 4
_RETRY_BACKOFF = 4  # 退避 4/8/12/16s：东财按 IP 限流时给足恢复窗口


@contextmanager
def _suppress_output():
    """屏蔽 akshare 的 tqdm 进度条（stderr）与零星 print（stdout）。"""
    saved = {}
    try:
        for name, fd in (("out", 1), ("err", 2)):
            try:
                saved[name] = (os.dup(fd), fd)
            except (OSError, ValueError):
                pass
        devnull = os.open(os.devnull, os.O_WRONLY)
        for _name, (_old, fd) in saved.items():
            os.dup2(devnull, fd)
        os.close(devnull)
        yield
    finally:
        for _name, (old, fd) in saved.items():
            os.dup2(old, fd)
            os.close(old)


def _is_retryable(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(
        k in msg
        for k in ("connection", "timeout", "aborted", "reset", "remotedisconnected",
                  "max retries", "temporarily")
    )


def _call(fn, *args, **kwargs) -> pd.DataFrame:
    """串行化 + 退避重试地调用 akshare 函数。"""
    last: Exception | None = None
    with _AK_LOCK:
        for attempt in range(_MAX_RETRIES + 1):
            try:
                with _suppress_output():
                    return fn(*args, **kwargs)
            except Exception as e:  # noqa: BLE001 akshare 抛各种 requests/json 异常
                last = e
                if attempt < _MAX_RETRIES and _is_retryable(e):
                    time.sleep(_RETRY_BACKOFF * (attempt + 1))
                    continue
                break
    raise DataSourceError(f"akshare {getattr(fn, '__name__', fn)} 调用失败: {last}") from last


# ── 代码格式转换 ──

def _parts(code: str) -> tuple[str, str]:
    """'sh.600000' → ('sh','600000')；裸 6 位按规则推断市场。"""
    if "." in code:
        m, n = code.split(".", 1)
        return m.lower(), n
    n = code
    m = "sh" if n[:1] in ("5", "6", "9") else "sz"
    return m, n


def _digits(code: str) -> str:
    return _parts(code)[1]


def _bs_code(market: str, num: str) -> str:
    return f"{market.lower()}.{num}"


# ── 取值清洗 ──

def _clean(df: pd.DataFrame) -> pd.DataFrame:
    """NaN/NaT → None，使 writer 的 _dec/_int/_date 落到空值分支。"""
    return df.astype(object).where(pd.notnull(df), None)


def _dstr(series: pd.Series) -> list:
    """日期列 → 'YYYY-MM-DD' 字符串（NaT→None）。"""
    out = []
    for v in series:
        if v is None or (isinstance(v, float) and pd.isna(v)) or v is pd.NaT:
            out.append(None)
            continue
        try:
            out.append(pd.Timestamp(v).strftime("%Y-%m-%d"))
        except (ValueError, TypeError):
            out.append(None)
    return out


def _vol_to_shares(series: pd.Series) -> list:
    """成交量 手 → 股（×100，取整）；NaN→None。"""
    out = []
    for v in series:
        if v is None or pd.isna(v):
            out.append(None)
        else:
            out.append(int(round(float(v) * 100)))
    return out


def _ymd(s: str) -> str:
    return s.replace("-", "")


# ── K 线 ──

_FREQ_MAP = {"d": "daily", "w": "weekly", "m": "monthly"}


def query_k_data(code: str, start_date: str, end_date: str, frequency: str) -> pd.DataFrame:
    """日/周/月 + 分钟 K 线（恒不复权，复权由读时计算）。"""
    if frequency in _FREQ_MAP:
        df = _call(ak.stock_zh_a_hist, symbol=_digits(code), period=_FREQ_MAP[frequency],
                   start_date=_ymd(start_date), end_date=_ymd(end_date), adjust="")
        if df is None or df.empty:
            raise NoDataFoundError(f"K线 {code} {frequency} {start_date}~{end_date}: 空")
        out = pd.DataFrame({
            "date": _dstr(df["日期"]),
            "code": code,
            "open": df["开盘"].to_numpy(),
            "high": df["最高"].to_numpy(),
            "low": df["最低"].to_numpy(),
            "close": df["收盘"].to_numpy(),
            "preclose": (df["收盘"] - df["涨跌额"]).to_numpy(),
            "volume": _vol_to_shares(df["成交量"]),
            "amount": df["成交额"].to_numpy(),
            "turn": df["换手率"].to_numpy(),
            "pctChg": df["涨跌幅"].to_numpy(),
            "tradestatus": 1,  # 东财只返回交易日
        })
        return _clean(out)

    # 分钟线 5/15/30/60 [降级：本机网络偶发不稳]
    df = _call(ak.stock_zh_a_hist_min_em, symbol=_digits(code), period=str(frequency),
               start_date=f"{start_date} 09:30:00", end_date=f"{end_date} 15:00:00", adjust="")
    if df is None or df.empty:
        raise NoDataFoundError(f"分钟K {code} {frequency} {start_date}~{end_date}: 空")
    times = [pd.Timestamp(v).strftime("%Y%m%d%H%M%S") for v in df["时间"]]
    out = pd.DataFrame({
        "time": times,
        "code": code,
        "open": df["开盘"].to_numpy(),
        "high": df["最高"].to_numpy(),
        "low": df["最低"].to_numpy(),
        "close": df["收盘"].to_numpy(),
        "volume": _vol_to_shares(df["成交量"]),
        "amount": df["成交额"].to_numpy(),
    })
    return _clean(out)


def query_adjust_factor(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """复权因子：raw 与 hfq 同源（东财），**逐交易日**精确产出
    back=hfq收盘/raw收盘、fore=back/back_今日。读时 raw×fore 即按构造精确复现东财前复权
    （raw×fore = raw·cum_d/cum_N = 东财 qfq）。

    关键：start_date/end_date 仅用于派发/去重，**恒抓到今日**——前复权归一到最新交易日
    （cum_N=今日），与东财 qfq 口径一致；新增除权后整段重算、整表 upsert 覆盖（同 baostock 设计）。
    逐日存储（非仅事件点）：raw/hfq 均为 2 位小数，逐日比值有舍入抖动，按事件去重不可靠，
    故存每日精确比值，读时取当日因子，无抖动、无需识别除权日。
    """
    digits = _digits(code)
    today = today_cst().strftime("%Y%m%d")

    def _close(adjust: str) -> pd.DataFrame:
        df = _call(ak.stock_zh_a_hist, symbol=digits, period="daily",
                   start_date="19901219", end_date=today, adjust=adjust)
        if df is None or df.empty:
            raise NoDataFoundError(f"复权因子 {code}: 无 K 线（未上市/已退市）")
        return df[["日期", "收盘"]].rename(columns={"收盘": adjust or "raw"})

    raw = _close("")
    qfq = _close("qfq")
    hfq = _close("hfq")
    m = raw.merge(qfq, on="日期").merge(hfq, on="日期")
    m = m[m["raw"] > 0].reset_index(drop=True)
    if m.empty:
        raise NoDataFoundError(f"复权因子 {code}: 收盘价缺失")
    # 直接逐日比值：读时 raw×fore=东财qfq、raw×back=东财hfq，按构造精确，无需归一假设
    return pd.DataFrame({
        "dividOperateDate": _dstr(m["日期"]),
        "foreAdjustFactor": (m["qfq"] / m["raw"]).to_numpy(),
        "backAdjustFactor": (m["hfq"] / m["raw"]).to_numpy(),
        "adjustFactor": (m["hfq"] / m["raw"]).to_numpy(),
    })


# ── 基本信息 / 分红 ──

_BASIC_CACHE: dict[str, dict[str, tuple]] = {}


def _load_basic(market: str) -> dict[str, tuple]:
    """交易所证券名录 → {6位代码: (简称, 上市日期)}，按市场缓存。
    （stock_individual_info_em 本机持续返回空，改用交易所名录；北交所 bse.cn 不可达。）"""
    if market in _BASIC_CACHE:
        return _BASIC_CACHE[market]
    fn = {"sh": ak.stock_info_sh_name_code, "sz": ak.stock_info_sz_name_code}.get(market)
    if fn is None:
        _BASIC_CACHE[market] = {}
        return {}
    df = _call(fn)
    code_col = next((c for c in df.columns if "代码" in c), None)
    name_col = next((c for c in df.columns if "简称" in c), None)
    date_col = next((c for c in df.columns if "上市" in c and "日期" in c), None)
    out = {}
    if code_col and name_col:
        for rec in df.to_dict("records"):
            num = str(rec[code_col]).zfill(6)
            ipo = rec.get(date_col) if date_col else None
            ipo_str = None
            if ipo not in (None, "", "-") and pd.notnull(ipo):
                try:
                    ipo_str = pd.Timestamp(str(ipo)).strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    ipo_str = None
            out[num] = (rec.get(name_col), ipo_str)
    _BASIC_CACHE[market] = out
    return out


def query_stock_basic(code: str) -> pd.DataFrame:
    """基本信息：交易所名录取 简称/上市日期。type 默认 1（股票）、status 默认 1（上市）、
    outDate 缺省。名录暂不可达时降级为仅 code（不阻断调用方，避免 502）。"""
    market, num = _parts(code)
    try:
        name, ipo_str = _load_basic(market).get(num, (None, None))
    except (DataSourceError, NoDataFoundError):
        name, ipo_str = None, None
    return pd.DataFrame([{
        "code": code,
        "code_name": name,
        "ipoDate": ipo_str,
        "outDate": None,
        "type": "1",
        "status": "1",
    }])


def query_dividend(code: str, year: str, year_type: str) -> pd.DataFrame:
    """东财分红配股详情，按报告期年份过滤。现金分红为每 10 股口径 → /10。"""
    df = _call(ak.stock_fhps_detail_em, symbol=_digits(code))
    if df is None or df.empty:
        raise NoDataFoundError(f"分红 {code} {year}: 空")
    df = df.copy()
    df["_y"] = [pd.Timestamp(v).year if pd.notnull(v) else None for v in df["报告期"]]
    df = df[df["_y"] == int(year)]
    if df.empty:
        raise NoDataFoundError(f"分红 {code} {year}: 当年无记录")

    def _per10(v):
        return float(v) / 10 if pd.notnull(v) else None

    rows = []
    for rec in df.to_dict("records"):
        plan = rec.get("预案公告日") or rec.get("最新公告日期") or rec.get("业绩披露日期")
        plan_str = pd.Timestamp(plan).strftime("%Y-%m-%d") if pd.notnull(plan) else None
        rows.append({
            "dividPlanAnnounceDate": plan_str,
            "dividRegistDate": (pd.Timestamp(rec["股权登记日"]).strftime("%Y-%m-%d")
                                if pd.notnull(rec.get("股权登记日")) else None),
            "dividOperateDate": (pd.Timestamp(rec["除权除息日"]).strftime("%Y-%m-%d")
                                 if pd.notnull(rec.get("除权除息日")) else None),
            "dividPayDate": None,
            "dividCashPsBeforeTax": _per10(rec.get("现金分红-现金分红比例")),
            "dividCashPsAfterTax": None,
            "dividStocksPs": _per10(rec.get("送转股份-送股比例")),
            "dividReserveToStockPs": _per10(rec.get("送转股份-转股比例")),
        })
    return pd.DataFrame(rows)


# ── 财报：新浪财务分析指标 → baostock 六类 ──

def _quarter_end(year: int, quarter: int) -> str:
    return f"{year}-{['03-31','06-30','09-30','12-31'][quarter-1]}"


# 各类别：baostock 消费键 → akshare 新浪指标列名
_FINA_MAP = {
    "profit": {"roeAvg": "净资产收益率(%)", "npMargin": "销售净利率(%)",
               "gpMargin": "销售毛利率(%)"},
    "operation": {"AssetTurnRatio": "总资产周转率(次)", "INVTurnRatio": "存货周转率(次)",
                  "CATurnRatio": "流动资产周转率(次)", "ARTurnRatio": "应收账款周转率(次)"},
    "growth": {"YOYEquity": "净资产增长率(%)", "YOYAsset": "总资产增长率(%)",
               "YOYNI": "净利润增长率(%)", "YOYRevenue": "主营业务收入增长率(%)"},
    "balance": {"currentRatio": "流动比率", "quickRatio": "速动比率",
                "assetLiabRatio": "资产负债率(%)", "cashRatio": "现金比率(%)"},
    "cash_flow": {"CFOToOR": "经营现金净流量对销售收入比率(%)",
                  "CFOToNP": "经营现金净流量与净利润的比率(%)"},
    "dupont": {"dupontROE": "加权净资产收益率(%)", "dupontNitogr": "销售净利率(%)",
               "dupontAssetTurn": "总资产周转率(次)"},
}


def query_fina_quarter(code: str, year: str, quarter: int) -> dict[str, dict]:
    """单季度六类财务数据（取自新浪财务分析指标，按报告期定位该季度）。"""
    df = _call(ak.stock_financial_analysis_indicator, symbol=_digits(code), start_year=str(year))
    if df is None or df.empty:
        return {}
    stat = _quarter_end(int(year), quarter)
    df = df.copy()
    df["_d"] = _dstr(df["日期"])
    sub = df[df["_d"] == stat]
    if sub.empty:
        return {}
    rec = sub.iloc[0].to_dict()

    def _num(col):
        v = rec.get(col)
        return None if v is None or pd.isna(v) else float(v)

    result: dict[str, dict] = {}
    for report_type, mapping in _FINA_MAP.items():
        metrics = {"statDate": stat}
        for bs_key, ak_col in mapping.items():
            val = _num(ak_col)
            if val is not None:
                metrics[bs_key] = val
        if len(metrics) > 1:
            result[report_type] = metrics
    return result


# ── 业绩快报 / 预告（按报告期取全市场再筛 code）──

def _quarter_ends_between(start_date: str, end_date: str) -> list[str]:
    s, e = datetime.strptime(start_date, "%Y-%m-%d").date(), datetime.strptime(end_date, "%Y-%m-%d").date()
    out = []
    for y in range(s.year, e.year + 1):
        for md in ("03-31", "06-30", "09-30", "12-31"):
            d = datetime.strptime(f"{y}-{md}", "%Y-%m-%d").date()
            if s <= d <= e:
                out.append(d.strftime("%Y%m%d"))
    return out


def query_performance_express(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """业绩快报：枚举区间内报告期，取全市场快报筛 code。"""
    digits = _digits(code)
    rows = []
    for period in _quarter_ends_between(start_date, end_date):
        try:
            df = _call(ak.stock_yjkb_em, date=period)
        except (DataSourceError, NoDataFoundError):
            continue
        if df is None or df.empty:
            continue
        sub = df[df["股票代码"] == digits]
        for rec in sub.to_dict("records"):
            stat = f"{period[:4]}-{period[4:6]}-{period[6:]}"
            rows.append({
                "performanceExpStatDate": stat,
                "performanceExpPubDate": (pd.Timestamp(rec["公告日期"]).strftime("%Y-%m-%d")
                                          if pd.notnull(rec.get("公告日期")) else None),
                "performanceExpressEPS": rec.get("每股收益"),
                "performanceExpressGRPS": rec.get("营业收入-营业收入"),
                "performanceExpressNPYOY": rec.get("净利润-同比增长"),
                "performanceExpressROEWa": rec.get("净资产收益率"),
            })
    if not rows:
        raise NoDataFoundError(f"业绩快报 {code} {start_date}~{end_date}: 空")
    return pd.DataFrame(rows)


def query_forecast(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """业绩预告：枚举区间内报告期，取全市场预告筛 code。"""
    digits = _digits(code)
    rows = []
    for period in _quarter_ends_between(start_date, end_date):
        try:
            df = _call(ak.stock_yjyg_em, date=period)
        except (DataSourceError, NoDataFoundError):
            continue
        if df is None or df.empty:
            continue
        sub = df[df["股票代码"] == digits]
        for rec in sub.to_dict("records"):
            stat = f"{period[:4]}-{period[4:6]}-{period[6:]}"
            rows.append({
                "profitForcastExpStatDate": stat,
                "profitForcastExpPubDate": (pd.Timestamp(rec["公告日期"]).strftime("%Y-%m-%d")
                                            if pd.notnull(rec.get("公告日期")) else None),
                "profitForcastType": rec.get("预告类型"),
                "profitForcastAbstract": rec.get("业绩变动"),
                "profitForcastChgPctUp": rec.get("业绩变动幅度"),
            })
    if not rows:
        raise NoDataFoundError(f"业绩预告 {code} {start_date}~{end_date}: 空")
    return pd.DataFrame(rows)


# ── 交易日历 / 列表 / 行业 / 指数 ──

_TRADE_DATES_CACHE: set[str] | None = None


def query_trade_dates(start_date: str, end_date: str) -> pd.DataFrame:
    """日历区间内每个自然日标注是否交易日。"""
    global _TRADE_DATES_CACHE
    if _TRADE_DATES_CACHE is None:
        df = _call(ak.tool_trade_date_hist_sina)
        _TRADE_DATES_CACHE = {pd.Timestamp(v).strftime("%Y-%m-%d") for v in df["trade_date"]}
    s = datetime.strptime(start_date, "%Y-%m-%d").date()
    e = datetime.strptime(end_date, "%Y-%m-%d").date()
    rows = []
    d = s
    while d <= e:
        ds = d.strftime("%Y-%m-%d")
        rows.append({"calendar_date": ds,
                     "is_trading_day": "1" if ds in _TRADE_DATES_CACHE else "0"})
        d = date.fromordinal(d.toordinal() + 1)
    return pd.DataFrame(rows)


def query_all_stock(date: str) -> pd.DataFrame:
    """[降级] 全市场列表用东财实时快照（仅当前态，历史快照退化）。"""
    df = _call(ak.stock_zh_a_spot_em)
    if df is None or df.empty:
        raise NoDataFoundError(f"全部股票 {date}: 空")
    rows = []
    for rec in df.to_dict("records"):
        num = str(rec.get("代码", "")).zfill(6)
        market = "sh" if num[:1] in ("5", "6", "9") else "sz"
        rows.append({"code": _bs_code(market, num), "code_name": rec.get("名称"),
                     "tradeStatus": "1"})
    return pd.DataFrame(rows)


def query_industry(date: str) -> pd.DataFrame:
    """[降级] 东财行业板块 → 成分股映射（遍历板块，调用量大、网络敏感）。"""
    boards = _call(ak.stock_board_industry_name_em)
    rows = []
    for b in boards["板块名称"].tolist():
        try:
            cons = _call(ak.stock_board_industry_cons_em, symbol=b)
        except (DataSourceError, NoDataFoundError):
            continue
        for rec in cons.to_dict("records"):
            num = str(rec.get("代码", "")).zfill(6)
            market = "sh" if num[:1] in ("5", "6", "9") else "sz"
            rows.append({"code": _bs_code(market, num), "code_name": rec.get("名称"),
                         "industry": b, "industryClassification": "东方财富"})
    if not rows:
        raise NoDataFoundError(f"行业分类 {date}: 空")
    return pd.DataFrame(rows)


_INDEX_CODE = {"sz50": "000016", "hs300": "000300", "zz500": "000905"}


def query_index_constituent(index_code: str, date: str) -> pd.DataFrame:
    """指数成分（中证官方，返回当前成分；历史时点退化）。"""
    df = _call(ak.index_stock_cons_csindex, symbol=_INDEX_CODE[index_code])
    if df is None or df.empty:
        raise NoDataFoundError(f"{index_code}成分股 {date}: 空")
    rows = []
    for rec in df.to_dict("records"):
        num = str(rec.get("成分券代码", "")).zfill(6)
        market = "sh" if "Shanghai" in str(rec.get("交易所英文名称", "")) else "sz"
        rows.append({"code": _bs_code(market, num), "code_name": rec.get("成分券名称")})
    return pd.DataFrame(rows)


# ── 宏观 ──

def _parse_cn_date(s) -> str | None:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    t = str(s).replace("年", "-").replace("月", "-").replace("日", "").rstrip("份-")
    try:
        return pd.Timestamp(t).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def query_macro(kind: str, start_date: str, end_date: str) -> pd.DataFrame:
    if kind in ("money_supply_month", "money_supply_year"):
        return _money_supply(kind, start_date, end_date)
    if kind == "rrr":
        return _rrr()
    # 存款/贷款基准利率已被 LPR 取代，无等价数据源
    raise NoDataFoundError(f"宏观 {kind}: akshare 无对应数据源（基准利率已被 LPR 取代）")


def _money_supply(kind: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = _call(ak.macro_china_money_supply)
    if df is None or df.empty:
        raise NoDataFoundError(f"宏观 {kind}: 空")
    recs = []
    for rec in df.to_dict("records"):
        ym = str(rec["月份"]).replace("年", "-").replace("月份", "").replace("月", "")
        try:
            ts = pd.Timestamp(ym + "-01")
        except (ValueError, TypeError):
            continue
        recs.append((ts, rec))
    recs.sort(key=lambda x: x[0])

    if kind == "money_supply_month":
        s = datetime.strptime(start_date + "-01", "%Y-%m-%d").date()
        e = datetime.strptime(end_date + "-01", "%Y-%m-%d").date()
        rows = []
        for ts, rec in recs:
            if not (s <= ts.date() <= e):
                continue
            rows.append({
                "statYear": ts.year, "statMonth": ts.month,
                "m0Month": rec.get("流通中的现金(M0)-数量(亿元)"),
                "m0YOY": rec.get("流通中的现金(M0)-同比增长"),
                "m0ChainRelative": rec.get("流通中的现金(M0)-环比增长"),
                "m1Month": rec.get("货币(M1)-数量(亿元)"),
                "m1YOY": rec.get("货币(M1)-同比增长"),
                "m1ChainRelative": rec.get("货币(M1)-环比增长"),
                "m2Month": rec.get("货币和准货币(M2)-数量(亿元)"),
                "m2YOY": rec.get("货币和准货币(M2)-同比增长"),
                "m2ChainRelative": rec.get("货币和准货币(M2)-环比增长"),
            })
        if not rows:
            raise NoDataFoundError(f"货币供应量月度 {start_date}~{end_date}: 空")
        return _clean(pd.DataFrame(rows))

    # money_supply_year：取每年 12 月值
    sy, ey = int(start_date), int(end_date)
    by_year = {}
    for ts, rec in recs:
        if sy <= ts.year <= ey and ts.month == 12:
            by_year[ts.year] = rec
    rows = []
    for y in sorted(by_year):
        rec = by_year[y]
        rows.append({
            "statYear": y,
            "m0Year": rec.get("流通中的现金(M0)-数量(亿元)"),
            "m0YearYOY": rec.get("流通中的现金(M0)-同比增长"),
            "m1Year": rec.get("货币(M1)-数量(亿元)"),
            "m1YearYOY": rec.get("货币(M1)-同比增长"),
            "m2Year": rec.get("货币和准货币(M2)-数量(亿元)"),
            "m2YearYOY": rec.get("货币和准货币(M2)-同比增长"),
        })
    if not rows:
        raise NoDataFoundError(f"货币供应量年度 {start_date}~{end_date}: 空")
    return _clean(pd.DataFrame(rows))


def _rrr() -> pd.DataFrame:
    df = _call(ak.macro_china_reserve_requirement_ratio)
    if df is None or df.empty:
        raise NoDataFoundError("准备金率: 空")
    rows = []
    for rec in df.to_dict("records"):
        rows.append({
            "pubDate": _parse_cn_date(rec.get("公布时间")),
            "effectiveDate": _parse_cn_date(rec.get("生效时间")),
            "bigInstitutionsRatioPre": rec.get("大型金融机构-调整前"),
            "bigInstitutionsRatioAfter": rec.get("大型金融机构-调整后"),
            "mediumInstitutionsRatioPre": rec.get("中小金融机构-调整前"),
            "mediumInstitutionsRatioAfter": rec.get("中小金融机构-调整后"),
        })
    return _clean(pd.DataFrame(rows))
