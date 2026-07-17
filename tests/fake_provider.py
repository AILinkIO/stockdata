"""FakeProvider：确定性假数据源，实现 Provider 协议。

- 记录所有调用（self.calls）供断言切片/顺序/续传行为。
- hooks：测试可注册回调 (method, kwargs)，用于注入异常。
- 交易日 = 工作日；数据窗口 [ipo, horizon]。
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from stockdata.provider.interface import NoDataFoundError

DAILY_COLS = [
    "date", "code", "open", "high", "low", "close", "preclose", "volume",
    "amount", "adjustflag", "turn", "tradestatus", "pctChg",
    "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM", "isST",
]


def _weekdays(start: date, end: date):
    d = start
    while d <= end:
        if d.isoweekday() <= 5:
            yield d
        d += timedelta(days=1)


class FakeProvider:
    def __init__(
        self,
        codes: tuple[str, ...] = ("sh.600000", "sz.000001"),
        ipo: date = date(2024, 1, 1),
        horizon: date = date(2026, 7, 16),
    ) -> None:
        self.codes = codes
        self.ipo = ipo
        self.horizon = horizon
        self.calls: list[tuple[str, dict]] = []
        self.hooks: list = []  # callable(method, kwargs)，可 raise

    def _rec(self, method: str, **kwargs):
        self.calls.append((method, kwargs))
        for hook in list(self.hooks):
            hook(method, kwargs)

    def calls_of(self, method: str, **match) -> list[dict]:
        return [
            kw for m, kw in self.calls
            if m == method and all(kw.get(k) == v for k, v in match.items())
        ]

    # ── K 线 ──

    def query_k_data(self, code, start_date, end_date, frequency):
        self._rec("query_k_data", code=code, start_date=start_date,
                  end_date=end_date, frequency=frequency)
        s = max(date.fromisoformat(start_date), self.ipo)
        e = min(date.fromisoformat(end_date), self.horizon)
        rows = []
        if frequency == "d":
            for d in _weekdays(s, e):
                rows.append([d.isoformat(), code, "10", "11", "9", "10.5", "10.2",
                             "1000", "10500.0", "3", "0.5", "1", "1.0",
                             "12.5", "1.2", "2.0", "8.0", "0"])
            cols = DAILY_COLS
        elif frequency == "w":
            for d in _weekdays(s, e):
                if d.isoweekday() == 5:  # 周五 = 周线锚点
                    rows.append([d.isoformat(), code, "10", "11", "9", "10.5",
                                 "5000", "52500.0", "3", "2.5", "2.0"])
            cols = ["date", "code", "open", "high", "low", "close", "volume",
                    "amount", "adjustflag", "turn", "pctChg"]
        else:  # 分钟线：每交易日 2 根
            for d in _weekdays(s, e):
                for hm in ("1000", "1500"):
                    t = f"{d:%Y%m%d}{hm}00000"
                    rows.append([d.isoformat(), t, code, "10", "11", "9", "10.5",
                                 "100", "1050.0", "3"])
            cols = ["date", "time", "code", "open", "high", "low", "close",
                    "volume", "amount", "adjustflag"]
        if not rows:
            raise NoDataFoundError(f"K线 {code} {frequency} 无数据")
        return pd.DataFrame(rows, columns=cols)

    # ── 按码其他 ──

    def query_adjust_factor(self, code, start_date, end_date):
        self._rec("query_adjust_factor", code=code)
        if code != self.codes[0]:
            raise NoDataFoundError("无复权因子")
        return pd.DataFrame(
            [[code, "2025-06-10", "1.0", "1.25", "1.25"]],
            columns=["code", "dividOperateDate", "foreAdjustFactor",
                     "backAdjustFactor", "adjustFactor"],
        )

    def query_stock_basic(self, code: str = ""):
        self._rec("query_stock_basic", code=code)
        rows = [
            [c, f"股票{i}", self.ipo.isoformat(), "", "1", "1"]
            for i, c in enumerate(self.codes)
            if not code or c == code
        ]
        if not rows:
            raise NoDataFoundError("无此证券")
        return pd.DataFrame(
            rows, columns=["code", "code_name", "ipoDate", "outDate", "type", "status"]
        )

    def query_dividend(self, code, year, year_type):
        self._rec("query_dividend", code=code, year=year, year_type=year_type)
        if year == "2025" and year_type == "operate" and code == self.codes[0]:
            return pd.DataFrame(
                [[code, "2025-05-01", "2025-06-10", "0.25"]],
                columns=["code", "dividPlanAnnounceDate", "dividOperateDate",
                         "dividCashPsBeforeTax"],
            )
        raise NoDataFoundError("无分红")

    def query_fina_quarter(self, code, year, quarter):
        self._rec("query_fina_quarter", code=code, year=year, quarter=quarter)
        if (year, quarter) == ("2026", 1):
            return {"profit": {"code": code, "statDate": "2026-03-31",
                               "pubDate": "2026-04-20", "roeAvg": "0.1"}}
        return {}

    def query_performance_express(self, code, start_date, end_date):
        self._rec("query_performance_express", code=code)
        raise NoDataFoundError("无业绩快报")

    def query_forecast(self, code, start_date, end_date):
        self._rec("query_forecast", code=code)
        raise NoDataFoundError("无业绩预告")

    # ── 市场级 ──

    def query_trade_dates(self, start_date, end_date):
        self._rec("query_trade_dates", start_date=start_date, end_date=end_date)
        s, e = date.fromisoformat(start_date), date.fromisoformat(end_date)
        rows = []
        d = s
        while d <= e:
            rows.append([d.isoformat(), "1" if d.isoweekday() <= 5 else "0"])
            d += timedelta(days=1)
        return pd.DataFrame(rows, columns=["calendar_date", "is_trading_day"])

    def query_all_stock(self, date):
        self._rec("query_all_stock", date=date)
        return pd.DataFrame(
            [[c, "1", f"股票{i}"] for i, c in enumerate(self.codes)],
            columns=["code", "tradeStatus", "code_name"],
        )

    def query_industry(self, date):
        self._rec("query_industry", date=date)
        return pd.DataFrame(
            [[date, c, f"股票{i}", "银行", "证监会行业"] for i, c in enumerate(self.codes)],
            columns=["updateDate", "code", "code_name", "industry",
                     "industryClassification"],
        )

    def query_index_constituent(self, index_code, date):
        self._rec("query_index_constituent", index_code=index_code, date=date)
        return pd.DataFrame(
            [[date, self.codes[0], "股票0"]],
            columns=["updateDate", "code", "code_name"],
        )

    def query_macro(self, kind, start_date, end_date):
        self._rec("query_macro", kind=kind)
        if kind in ("deposit_rate", "loan_rate"):
            return pd.DataFrame([["2025-10-01", "0.35"]], columns=["pubDate", "rate"])
        if kind == "rrr":
            return pd.DataFrame(
                [["2025-09-01", "2025-09-15", "8.0"]],
                columns=["pubDate", "effectiveDate", "ratio"],
            )
        if kind == "money_supply_month":
            return pd.DataFrame([["2026", "05", "300"]],
                                columns=["statYear", "statMonth", "m2"])
        return pd.DataFrame([["2025", "290"]], columns=["statYear", "m2"])

    def logout(self):
        self._rec("logout")
