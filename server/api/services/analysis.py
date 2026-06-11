"""个股分析报告（自 src/core/analysis.py 移植到新数据访问层，指标口径不变）。"""

from datetime import timedelta

from api.services import financial, kline, market
from api.services.readthrough import today


def _metric(rows: list[dict], report_type: str, key: str):
    for r in rows:
        if r["report_type"] == report_type:
            return r["metrics"].get(key)
    return None


def build_stock_analysis_report(code: str, analysis_type: str) -> str:
    """生成个股数据分析报告（Markdown）。analysis_type: fundamental | technical | comprehensive。"""
    if analysis_type not in ("fundamental", "technical", "comprehensive"):
        raise ValueError("analysis_type 必须为 fundamental / technical / comprehensive")

    basic = market.get_stock_basic(code)
    stock_name = (basic or {}).get("code_name") or code

    report = f"# {stock_name} 数据分析报告\n\n"
    report += ("## 免责声明\n本报告基于公开数据生成，仅供参考，不构成投资建议。"
               "投资决策需基于个人风险承受能力和研究。\n\n")

    if basic:
        industry_rows = market.get_industry(today() - timedelta(days=1), code=code) or \
            market.get_industry(today(), code=code)
        industry = industry_rows[0]["industry"] if industry_rows else "未知"
        report += "## 公司基本信息\n"
        report += f"- 股票代码: {code}\n"
        report += f"- 股票名称: {stock_name}\n"
        report += f"- 所属行业: {industry}\n"
        report += f"- 上市日期: {basic.get('ipo_date') or '未知'}\n\n"

    if analysis_type in ("fundamental", "comprehensive"):
        t = today()
        year, quarter = t.year, (t.month - 1) // 3 + 1
        rows = financial.get_quarterly(code, year, quarter)
        if not rows:  # 当季未披露则回退上一季度
            year, quarter = (year - 1, 4) if quarter == 1 else (year, quarter - 1)
            rows = financial.get_quarterly(code, year, quarter)

        if rows:
            report += f"## 基本面指标分析 ({year}年第{quarter}季度)\n\n"
            report += "### 盈利能力指标\n"
            if (v := _metric(rows, "profit", "roeAvg")) is not None:
                report += f"- ROE(净资产收益率): {v}%\n"
            if (v := _metric(rows, "profit", "npMargin")) is not None:
                report += f"- 销售净利率: {v}%\n"
            report += "\n### 成长能力指标\n"
            if (v := _metric(rows, "growth", "YOYEquity")) is not None:
                report += f"- 净资产同比增长: {v}%\n"
            if (v := _metric(rows, "growth", "YOYAsset")) is not None:
                report += f"- 总资产同比增长: {v}%\n"
            if (v := _metric(rows, "growth", "YOYNI")) is not None:
                report += f"- 净利润同比增长: {v}%\n"
            report += "\n### 偿债能力指标\n"
            if (v := _metric(rows, "balance", "currentRatio")) is not None:
                report += f"- 流动比率: {v}\n"
            if (v := _metric(rows, "balance", "assetLiabRatio")) is not None:
                report += f"- 资产负债率: {v}%\n"

    if analysis_type in ("technical", "comprehensive"):
        end = today()
        bars = kline.get_kline(code, end - timedelta(days=180), end)
        closes = [float(b["close"]) for b in bars if b.get("close") is not None]
        if closes:
            report += "\n## 技术面简析（近180日）\n"
            change = (closes[-1] - closes[0]) / closes[0] * 100 if closes[0] else 0
            report += f"- 区间涨跌幅: {change:.2f}%\n"
            if len(closes) >= 20:
                ma20 = sum(closes[-20:]) / 20
                report += f"- 20日均线: {ma20:.2f}\n"

    return report
