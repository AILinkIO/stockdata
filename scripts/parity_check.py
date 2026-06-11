"""
新旧实现对照验证（迁移计划阶段 6，最终结果 25/25 PASS）。

⚠️ 历史脚本：依赖已删除的旧 MCP 实现（src/），仅可在
`git checkout pre-restructure` 后的工作区运行，保留作迁移记录。

旧：src.data_source.active_data_source（CachedDataSource + BaostockDataSource，线程版）
新：REST API（http://127.0.0.1:8000，PG + fetcher）

逐项比对数值内容（忽略表示格式差异：旧为字符串 DataFrame，新为类型化 JSON）。
运行前提：API 与 fetcher worker 已启动。

    uv run python scripts/parity_check.py
"""

import json
import os
import sys
import urllib.request
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE = "http://127.0.0.1:8000"
CODE = "sh.600000"
PASS, FAIL = [], []


def api(path: str):
    with urllib.request.urlopen(f"{BASE}{path}", timeout=120) as r:
        return json.loads(r.read())


def close_enough(a, b, tol=1e-6) -> bool:
    if a in (None, "") and b in (None, ""):
        return True
    try:
        return abs(Decimal(str(a)) - Decimal(str(b))) <= Decimal(str(tol))
    except Exception:
        return str(a) == str(b)


def check(name: str, ok: bool, detail: str = ""):
    (PASS if ok else FAIL).append(name)
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail and not ok else ""))


def main() -> int:
    from src.data_source import active_data_source as old

    # ── 1. K 线（不复权/前复权/后复权，跨除权日） ──
    for flag, label in (("3", "raw"), ("2", "qfq"), ("1", "hfq")):
        df = old.get_historical_k_data(CODE, "2024-07-01", "2024-07-31", adjust_flag=flag)
        new = api(f"/api/v1/stocks/{CODE}/kline?start_date=2024-07-01&end_date=2024-07-31&adjust_flag={flag}")
        ok = len(df) == len(new) and all(
            close_enough(o, n["close"], 1e-4)
            for o, n in zip(df["close"], new)
        )
        check(f"kline {label} ({len(df)} bars)", ok)

    # ── 2. 周线 / 月线 ──
    for freq in ("w", "m"):
        df = old.get_historical_k_data(CODE, "2024-01-01", "2024-12-31", frequency=freq)
        new = api(f"/api/v1/stocks/{CODE}/kline?start_date=2024-01-01&end_date=2024-12-31&frequency={freq}")
        ok = len(df) == len(new) and all(
            close_enough(o, n["close"]) for o, n in zip(df["close"], new)
        )
        check(f"kline freq={freq} ({len(df)} bars)", ok)

    # ── 3. 复权因子 ──
    df = old.get_adjust_factor_data(CODE, "2008-01-01", "2024-12-31")
    new = api(f"/api/v1/stocks/{CODE}/adjust-factors?start_date=2008-01-01&end_date=2024-12-31")
    ok = len(df) == len(new) and all(
        close_enough(o, n["fore_adjust_factor"])
        for o, n in zip(df["foreAdjustFactor"], new)
    )
    check(f"adjust_factor ({len(df)} events)", ok)

    # ── 4. 基本信息 ──
    df = old.get_stock_basic_info(CODE)
    new = api(f"/api/v1/stocks/{CODE}/basic")
    check("stock_basic", df["code_name"].iloc[0] == new["code_name"]
          and df["ipoDate"].iloc[0] == new["ipo_date"])

    # ── 5. 分红 ──
    df = old.get_dividend_data(CODE, "2023")
    new = api(f"/api/v1/stocks/{CODE}/dividends?year=2023")
    ok = len(df) == len(new) and all(
        close_enough(o, n["cash_ps_before_tax"])
        for o, n in zip(df["dividCashPsBeforeTax"], new)
    )
    check(f"dividend 2023 ({len(df)} rows)", ok)

    # ── 6. 六类季度财报（2024Q3） ──
    quarterly = {
        "profit": old.get_profit_data, "operation": old.get_operation_data,
        "growth": old.get_growth_data, "balance": old.get_balance_data,
        "cash_flow": old.get_cash_flow_data, "dupont": old.get_dupont_data,
    }
    for rtype, fn in quarterly.items():
        df = fn(CODE, "2024", 3)
        new = api(f"/api/v1/stocks/{CODE}/financials/{rtype}?year=2024&quarter=3")
        old_row = df.iloc[0].to_dict()
        metrics = new[0]["metrics"] if new else {}
        shared = [k for k in old_row if k in metrics]
        ok = bool(new) and all(close_enough(old_row[k], metrics[k]) for k in shared)
        check(f"financial {rtype} 2024Q3 ({len(shared)} metrics)", ok)

    # ── 7. 业绩快报 / 预告 ──
    df = old.get_performance_express_report(CODE, "2024-01-01", "2024-12-31")
    new = api(f"/api/v1/stocks/{CODE}/financials/express?start_date=2024-01-01&end_date=2024-12-31")
    ok = len(df) == len(new) and all(
        close_enough(o, n.get("performanceExpressROEWa"))
        for o, n in zip(df["performanceExpressROEWa"], new)
    )
    check(f"express ({len(df)} rows)", ok)

    # ── 8. 综合财务指标 ──
    # 注：旧实现不检查 start 边界（范围 7/1~12/31 会返回全年 Q1~Q4），属旧实现缺陷；
    # 新实现只返回范围内季度。此处按 (year, quarter) 配对比较指标值。
    df = old.get_fina_indicator(CODE, "2024-07-01", "2024-12-31")
    new = api(f"/api/v1/stocks/{CODE}/financials/indicator?start_date=2024-07-01&end_date=2024-12-31")
    new_by_q = {(int(r["year"]), int(r["quarter"])): r for r in new}
    matched, ok = 0, bool(new)
    for _, old_row in df.iterrows():
        key = (int(old_row["year"]), int(old_row["quarter"]))
        if key not in new_by_q:
            continue
        new_row = new_by_q[key]
        shared = [k for k in old_row.index
                  if k in new_row and k not in ("code", "year", "quarter")]
        matched += 1
        ok = ok and len(shared) > 30 and all(
            close_enough(old_row[k], new_row[k]) for k in shared
        )
    ok = ok and matched == len(new)  # 范围内季度全部配对成功
    check(f"fina_indicator ({matched} quarters matched)", ok)

    # ── 9. 交易日历 ──
    df = old.get_trade_dates("2024-01-01", "2024-12-31")
    new = api("/api/v1/market/trade-calendar?start_date=2024-01-01&end_date=2024-12-31")
    old_trading = {r["calendar_date"] for _, r in df.iterrows() if r["is_trading_day"] == "1"}
    new_trading = {r["calendar_date"] for r in new if r["is_trading_day"]}
    check(f"trade_calendar ({len(old_trading)} trading days)", old_trading == new_trading)

    # ── 10. 全部股票列表（昨日） ──
    snap = api("/api/v1/market/stocks")
    snap_date = snap[0]["snap_date"] if snap else None
    df = old.get_all_stock(snap_date)
    check(f"stock_list {snap_date}", len(df) == len(snap)
          and set(df["code"]) == {r["code"] for r in snap})

    # ── 11. 指数成分股 ──
    for index_code, fn in (("sz50", old.get_sz50_stocks),
                           ("hs300", old.get_hs300_stocks),
                           ("zz500", old.get_zz500_stocks)):
        df = fn()
        new = api(f"/api/v1/indices/{index_code}/constituents")
        check(f"index {index_code} ({len(df)})", set(df["code"]) == {r["code"] for r in new})

    # ── 12. 行业分类（单只） ──
    df = old.get_stock_industry(CODE)
    new = api(f"/api/v1/industries?code={CODE}")
    check("industry", bool(new) and df["industry"].iloc[0] == new[0]["industry"])

    # ── 13. 宏观 ──
    df = old.get_deposit_rate_data("2015-01-01", "2015-12-31")
    new = api("/api/v1/macro/deposit-rate?start_date=2015-01-01&end_date=2015-12-31")
    ok = len(df) == len(new) and all(
        close_enough(o, n["demand_deposit_rate"])
        for o, n in zip(df["demandDepositRate"], new)
    )
    check(f"deposit_rate 2015 ({len(df)} rows)", ok)

    df = old.get_money_supply_data_month("2025-01", "2025-06")
    new = api("/api/v1/macro/money-supply/month?start_date=2025-01-01&end_date=2025-06-30")
    ok = len(df) == len(new) and all(
        close_enough(o, n["m2_month"]) for o, n in zip(df["m2Month"], new)
    )
    check(f"money_supply_month ({len(df)} rows)", ok)

    df = old.get_required_reserve_ratio_data("2010-01-01", "2015-12-31")
    new = api("/api/v1/macro/rrr?start_date=2010-01-01&end_date=2015-12-31")
    check(f"rrr ({len(df)} rows)", len(df) == len(new))

    print(f"\n══ 对照结果: {len(PASS)} PASS / {len(FAIL)} FAIL ══")
    if FAIL:
        print("失败项:", FAIL)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
