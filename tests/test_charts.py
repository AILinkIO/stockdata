"""复权计算与图表 option 单测（手算样例）。"""

import subprocess
import sys
from datetime import date

import pandas as pd

from stockdata.web.charts import apply_adjust, kline_option


def _bars():
    return pd.DataFrame({
        "t": [date(2025, 6, 9), date(2025, 6, 10)],
        "open": [10.0, 8.0],
        "high": [10.5, 8.5],
        "low": [9.5, 7.9],
        "close": [10.0, 8.2],
        "volume": [1000, 1200],
        "amount": [10000.0, 9800.0],
    })


def _factors():
    # 2025-06-10 除权：back 因子 1.25（除权前 B=1）
    return pd.DataFrame({
        "divid_operate_date": [date(2025, 6, 10)],
        "back_adjust_factor": [1.25],
    })


def test_back_adjust():
    out = apply_adjust(_bars(), _factors(), "back")
    assert out["close"].tolist() == [10.0, 8.2 * 1.25]   # 除权前 ×1，除权后 ×1.25


def test_fore_adjust():
    out = apply_adjust(_bars(), _factors(), "fore")
    # 前复权 = raw × B(t)/B(latest)：除权前 10/1.25=8，除权后原价不变
    assert out["close"].tolist() == [8.0, 8.2]


def test_none_adjust_returns_raw():
    out = apply_adjust(_bars(), _factors(), "none")
    assert out["close"].tolist() == [10.0, 8.2]


def test_no_factors_returns_raw():
    out = apply_adjust(_bars(), pd.DataFrame(columns=["divid_operate_date",
                                                      "back_adjust_factor"]), "fore")
    assert out["close"].tolist() == [10.0, 8.2]


def test_kline_option_shape():
    opt = kline_option("sh.600000", "浦发银行", "d", _bars())
    assert opt["series"][0]["type"] == "candlestick"
    # ECharts K 线数据顺序 [open, close, low, high]
    assert opt["series"][0]["data"][0] == [10.0, 10.0, 9.5, 10.5]
    assert opt["series"][1]["type"] == "bar"
    assert len(opt["xAxis"][0]["data"]) == 2


def test_web_never_imports_baostock():
    """web 模块（页面/图表/查询）绝不触碰 baostock。子进程中验证导入闭包。"""
    code = (
        "import stockdata.web.app, stockdata.web.charts, stockdata.db.queries, "
        "stockdata.web.pages.home, stockdata.web.pages.chart, stockdata.web.pages.sync, "
        "sys; assert 'baostock' not in sys.modules, 'web 导入链引入了 baostock!'"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
