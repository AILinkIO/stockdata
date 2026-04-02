"""
综合分析报告生成模块。

组合多个数据源调用（基本信息、财务报表、K线行情），
生成结构化的个股分析报告（Markdown 格式）。

支持三种分析模式：
- fundamental: 基本面分析（盈利、成长、偿债指标）
- technical:   技术面分析（近180日涨跌幅、均线）
- comprehensive: 综合分析（基本面 + 技术面）
"""
from datetime import datetime, timedelta

from src.providers.interface import FinancialDataSource


def build_stock_analysis_report(data_source: FinancialDataSource, *, code: str, analysis_type: str) -> str:
    """生成个股数据分析报告。

    Args:
        data_source: 金融数据源实例
        code: Baostock 格式股票代码，如 'sh.600000'
        analysis_type: 分析类型，'fundamental' | 'technical' | 'comprehensive'

    Returns:
        Markdown 格式的分析报告字符串
    """
    # --- 获取基本信息（所有分析模式都需要） ---
    basic_info = data_source.get_stock_basic_info(code=code)

    # --- 基本面数据：盈利、成长、偿债、杜邦 ---
    if analysis_type in ["fundamental", "comprehensive"]:
        recent_year = datetime.now().strftime("%Y")
        recent_quarter = (datetime.now().month - 1) // 3 + 1
        if recent_quarter < 1:
            recent_year = str(int(recent_year) - 1)
            recent_quarter = 4

        profit_data = data_source.get_profit_data(code=code, year=recent_year, quarter=recent_quarter)
        growth_data = data_source.get_growth_data(code=code, year=recent_year, quarter=recent_quarter)
        balance_data = data_source.get_balance_data(code=code, year=recent_year, quarter=recent_quarter)
        dupont_data = data_source.get_dupont_data(code=code, year=recent_year, quarter=recent_quarter)
    else:
        profit_data = growth_data = balance_data = dupont_data = None

    # --- 技术面数据：近180日 K 线 ---
    if analysis_type in ["technical", "comprehensive"]:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
        price_data = data_source.get_historical_k_data(code=code, start_date=start_date, end_date=end_date)
    else:
        price_data = None

    # --- 组装报告 ---
    stock_name = basic_info['code_name'].values[0] if not basic_info.empty else code
    report = f"# {stock_name} 数据分析报告\n\n"
    report += "## 免责声明\n本报告基于公开数据生成，仅供参考，不构成投资建议。投资决策需基于个人风险承受能力和研究。\n\n"

    # 公司基本信息
    if not basic_info.empty:
        report += "## 公司基本信息\n"
        report += f"- 股票代码: {code}\n"
        report += f"- 股票名称: {basic_info['code_name'].values[0]}\n"
        report += f"- 所属行业: {basic_info['industry'].values[0] if 'industry' in basic_info.columns else '未知'}\n"
        report += f"- 上市日期: {basic_info['ipoDate'].values[0] if 'ipoDate' in basic_info.columns else '未知'}\n\n"

    # 基本面指标
    if analysis_type in ["fundamental", "comprehensive"] and profit_data is not None and not profit_data.empty:
        report += f"## 基本面指标分析 ({recent_year}年第{recent_quarter}季度)\n\n"
        report += "### 盈利能力指标\n"
        if 'roeAvg' in profit_data.columns:
            report += f"- ROE(净资产收益率): {profit_data['roeAvg'].values[0]}%\n"
        if 'npMargin' in profit_data.columns:
            report += f"- 销售净利率: {profit_data['npMargin'].values[0]}%\n"

        if growth_data is not None and not growth_data.empty:
            report += "\n### 成长能力指标\n"
            if 'YOYEquity' in growth_data.columns:
                report += f"- 净资产同比增长: {growth_data['YOYEquity'].values[0]}%\n"
            if 'YOYAsset' in growth_data.columns:
                report += f"- 总资产同比增长: {growth_data['YOYAsset'].values[0]}%\n"
            if 'YOYNI' in growth_data.columns:
                report += f"- 净利润同比增长: {growth_data['YOYNI'].values[0]}%\n"

        if balance_data is not None and not balance_data.empty:
            report += "\n### 偿债能力指标\n"
            if 'currentRatio' in balance_data.columns:
                report += f"- 流动比率: {balance_data['currentRatio'].values[0]}\n"
            if 'assetLiabRatio' in balance_data.columns:
                report += f"- 资产负债率: {balance_data['assetLiabRatio'].values[0]}%\n"

    # 技术面简析
    if analysis_type in ["technical", "comprehensive"] and price_data is not None and not price_data.empty:
        report += "\n## 技术面简析（近180日）\n"
        latest_price = price_data['close'].iloc[-1]
        start_price = price_data['close'].iloc[0]
        price_change = ((latest_price - start_price) / start_price) * 100 if start_price else 0
        report += f"- 区间涨跌幅: {price_change:.2f}%\n"
        if 'close' in price_data.columns and price_data.shape[0] >= 20:
            ma20 = price_data['close'].astype(float).rolling(window=20).mean().iloc[-1]
            report += f"- 20日均线: {ma20:.2f}\n"

    return report
