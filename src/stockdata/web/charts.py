"""K 线图：读时复权计算 + ECharts option 组装。

复权只用 back 因子推导（存量 fore 因子会因新分红过期，back 因子永不过期）：
- 后复权价 = 原始价 × B(t)
- 前复权价 = 原始价 × B(t) / B(latest)
其中 B(t) = 交易时刻所处最近一次除权除息日的 back_adjust_factor（as-of 对齐），
首个除权日之前 B = 1。
"""

from __future__ import annotations

import pandas as pd

ADJUST_NONE = "none"
ADJUST_FORE = "fore"   # 前复权
ADJUST_BACK = "back"   # 后复权

UP_COLOR = "#ef232a"    # A 股红涨
DOWN_COLOR = "#14b143"  # 绿跌


def apply_adjust(bars: pd.DataFrame, factors: pd.DataFrame, adjust: str) -> pd.DataFrame:
    """bars: load_kline 结果（t/open/high/low/close/volume/amount）。返回复权后副本。"""
    if bars.empty or adjust == ADJUST_NONE or factors.empty:
        return bars
    bars = bars.copy()
    f = factors.copy()
    f["divid_operate_date"] = pd.to_datetime(f["divid_operate_date"])
    f["back_adjust_factor"] = f["back_adjust_factor"].astype(float)
    f = f.sort_values("divid_operate_date")

    bar_dates = pd.to_datetime(pd.Series([_to_date(t) for t in bars["t"]], index=bars.index))
    merged = pd.merge_asof(
        pd.DataFrame({"bar_date": bar_dates}).sort_index(),
        f.rename(columns={"divid_operate_date": "bar_date"}),
        on="bar_date",
    )
    b = merged["back_adjust_factor"].fillna(1.0).to_numpy()
    if adjust == ADJUST_FORE:
        b = b / f["back_adjust_factor"].iloc[-1]
    for col in ("open", "high", "low", "close"):
        bars[col] = bars[col].astype(float) * b
    return bars


def _to_date(t):
    return t.date() if hasattr(t, "date") else t


def _fmt_axis(t, minute: bool) -> str:
    if minute:
        return t.strftime("%m-%d %H:%M")
    return t.isoformat() if hasattr(t, "isoformat") else str(t)


def kline_option(code: str, name: str, frequency: str, bars: pd.DataFrame) -> dict:
    """ECharts option：candlestick + 成交量副图 + dataZoom。category 轴天然跳过休市。"""
    minute = frequency in ("5", "30")
    x = [_fmt_axis(t, minute) for t in bars["t"]]
    # ECharts candlestick 数据顺序：[open, close, low, high]
    candles = [
        [round(float(o), 4), round(float(c), 4), round(float(lo), 4), round(float(h), 4)]
        for o, c, lo, h in zip(bars["open"], bars["close"], bars["low"], bars["high"])
    ]
    volumes = [
        {
            "value": int(v) if v is not None else 0,
            "itemStyle": {
                "color": UP_COLOR if float(c) >= float(o) else DOWN_COLOR,
                "opacity": 0.6,
            },
        }
        for v, o, c in zip(bars["volume"], bars["open"], bars["close"])
    ]
    freq_label = {"5": "5分", "30": "30分", "d": "日K", "w": "周K"}[frequency]
    return {
        "animation": False,
        "title": {"text": f"{code} {name} · {freq_label}", "left": 8, "top": 4},
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {"type": "cross"},
        },
        "axisPointer": {"link": [{"xAxisIndex": "all"}]},
        "grid": [
            {"left": 64, "right": 16, "top": 48, "height": "56%"},
            {"left": 64, "right": 16, "top": "74%", "height": "16%"},
        ],
        "xAxis": [
            {"type": "category", "data": x, "gridIndex": 0,
             "boundaryGap": True, "axisLine": {"onZero": False}},
            {"type": "category", "data": x, "gridIndex": 1,
             "boundaryGap": True, "axisLabel": {"show": False}},
        ],
        "yAxis": [
            {"scale": True, "gridIndex": 0, "splitArea": {"show": True}},
            {"gridIndex": 1, "axisLabel": {"show": False},
             "splitLine": {"show": False}},
        ],
        "dataZoom": [
            {"type": "inside", "xAxisIndex": [0, 1], "start": 60, "end": 100},
            {"type": "slider", "xAxisIndex": [0, 1], "bottom": 4, "start": 60, "end": 100},
        ],
        "series": [
            {
                "name": "K线",
                "type": "candlestick",
                "data": candles,
                "xAxisIndex": 0,
                "yAxisIndex": 0,
                "itemStyle": {
                    "color": UP_COLOR, "color0": DOWN_COLOR,
                    "borderColor": UP_COLOR, "borderColor0": DOWN_COLOR,
                },
            },
            {
                "name": "成交量",
                "type": "bar",
                "data": volumes,
                "xAxisIndex": 1,
                "yAxisIndex": 1,
            },
        ],
    }
