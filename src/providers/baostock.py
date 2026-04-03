"""
Baostock 数据源实现。

基于 Baostock Python SDK 实现 FinancialDataSource 接口，
提供 A 股行情、财务报表、宏观经济、指数成分股等数据查询能力。
通过 execute() 提交请求到队列，worker 线程串行执行。
"""

import logging
from datetime import datetime
from typing import Optional

import baostock as bs
import pandas as pd

from .interface import (
    DataSourceError,
    LoginError,
    NoDataFoundError,
    FinancialDataSource,
)
from .context import execute

logger = logging.getLogger(__name__)

# K 线默认返回字段（按频率区分）
_DAILY_K_FIELDS = [
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "preclose",
    "volume",
    "amount",
    "adjustflag",
    "turn",
    "tradestatus",
    "pctChg",
    "peTTM",
    "pbMRQ",
    "psTTM",
    "pcfNcfTTM",
    "isST",
]
_WEEKLY_MONTHLY_K_FIELDS = [
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjustflag",
    "turn",
    "pctChg",
]
_MINUTE_K_FIELDS = [
    "date",
    "time",
    "code",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjustflag",
]


def _default_k_fields(frequency: str) -> list[str]:
    """根据 K 线频率返回对应的默认字段列表。"""
    if frequency in ("w", "m"):
        return _WEEKLY_MONTHLY_K_FIELDS
    if frequency in ("5", "15", "30", "60"):
        return _MINUTE_K_FIELDS
    return _DAILY_K_FIELDS


# get_fina_indicator 聚合查询的各类别配置：(bs 查询函数, 字段前缀)
_FINA_CATEGORIES = [
    (bs.query_profit_data, "profit"),
    (bs.query_operation_data, "operation"),
    (bs.query_growth_data, "growth"),
    (bs.query_balance_data, "balance"),
    (bs.query_cash_flow_data, "cashflow"),
    (bs.query_dupont_data, "dupont"),
]


# ── 通用查询工具函数 ──


def _check_api_error(rs, description: str) -> None:
    """检查 Baostock API 返回的错误码，失败时抛出对应异常。"""
    if rs.error_code == "0":
        return
    msg = f"{description}: {rs.error_msg} (code: {rs.error_code})"
    if "no record found" in rs.error_msg.lower() or rs.error_code == "10002":
        raise NoDataFoundError(msg)
    raise DataSourceError(msg)


def _collect_rows(rs, description: str) -> pd.DataFrame:
    """从 Baostock ResultSet 收集全部数据行，转为 DataFrame。"""
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        raise NoDataFoundError(f"{description}: 查询结果为空")
    df = pd.DataFrame(rows, columns=rs.fields)
    logger.info(f"{description}: 获取 {len(df)} 条记录")
    return df


def _query(bs_func, description: str, **kwargs) -> pd.DataFrame:
    """统一的 Baostock 查询流程：调用 API → 校验 → 收集数据。

    通过 execute() 提交到请求队列，worker 线程串行执行。
    会话失效时 worker 自动重连并重试一次。
    """
    logger.info(f"正在查询 {description}")

    def _do_query():
        rs = bs_func(**kwargs)
        _check_api_error(rs, description)
        return _collect_rows(rs, description)

    try:
        return execute(_do_query)
    except (LoginError, NoDataFoundError, DataSourceError, ValueError):
        raise
    except Exception as e:
        raise DataSourceError(f"{description}: 未预期错误 - {e}") from e


def _query_fina_category(
    bs_func, prefix: str, code: str, year: str, quarter: int
) -> dict:
    """查询单个财务指标类别，返回带前缀的字段字典。查询失败时返回空字典。"""
    try:

        def _do_query():
            rs = bs_func(code=code, year=year, quarter=quarter)
            if rs.error_code != "0" or not rs.next():
                return {}
            row = rs.get_row_data()
            return {
                f"{prefix}_{field}": row[i]
                for i, field in enumerate(rs.fields)
                if i < len(row)
            }

        return execute(_do_query)
    except Exception as e:
        logger.debug(f"获取 {prefix} 数据失败 ({code} {year}Q{quarter}): {e}")
        return {}


# ── BaostockDataSource 实现 ──


class BaostockDataSource(FinancialDataSource):
    """基于 Baostock SDK 的金融数据源实现。"""

    # ── 股票行情 ──

    def get_historical_k_data(
        self,
        code: str,
        start_date: str,
        end_date: str,
        frequency: str = "d",
        adjust_flag: str = "3",
        fields: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """获取历史 K 线数据。"""
        field_str = ",".join(fields or _default_k_fields(frequency))
        return _query(
            bs.query_history_k_data_plus,
            f"K线 {code} {start_date}~{end_date}",
            code=code,
            fields=field_str,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            adjustflag=adjust_flag,
        )

    def get_stock_basic_info(
        self, code: str, fields: Optional[list[str]] = None
    ) -> pd.DataFrame:
        """获取股票基本信息，可选过滤返回字段。"""
        df = _query(bs.query_stock_basic, f"基本信息 {code}", code=code)
        if not fields:
            return df
        available = [col for col in fields if col in df.columns]
        if not available:
            raise ValueError(f"请求的字段 {fields} 均不存在于查询结果中")
        return df[available]

    def get_dividend_data(
        self, code: str, year: str, year_type: str = "report"
    ) -> pd.DataFrame:
        """获取分红送转数据。"""
        return _query(
            bs.query_dividend_data,
            f"分红 {code} {year}",
            code=code,
            year=year,
            yearType=year_type,
        )

    def get_adjust_factor_data(
        self, code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """获取复权因子数据。"""
        return _query(
            bs.query_adjust_factor,
            f"复权因子 {code} {start_date}~{end_date}",
            code=code,
            start_date=start_date,
            end_date=end_date,
        )

    # ── 财务报表（按季度） ──

    def get_profit_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        """获取季度盈利能力数据。"""
        return _query(
            bs.query_profit_data,
            f"盈利能力 {code} {year}Q{quarter}",
            code=code,
            year=year,
            quarter=quarter,
        )

    def get_operation_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        """获取季度营运能力数据。"""
        return _query(
            bs.query_operation_data,
            f"营运能力 {code} {year}Q{quarter}",
            code=code,
            year=year,
            quarter=quarter,
        )

    def get_growth_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        """获取季度成长能力数据。"""
        return _query(
            bs.query_growth_data,
            f"成长能力 {code} {year}Q{quarter}",
            code=code,
            year=year,
            quarter=quarter,
        )

    def get_balance_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        """获取季度偿债能力数据。"""
        return _query(
            bs.query_balance_data,
            f"偿债能力 {code} {year}Q{quarter}",
            code=code,
            year=year,
            quarter=quarter,
        )

    def get_cash_flow_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        """获取季度现金流量数据。"""
        return _query(
            bs.query_cash_flow_data,
            f"现金流量 {code} {year}Q{quarter}",
            code=code,
            year=year,
            quarter=quarter,
        )

    def get_dupont_data(self, code: str, year: str, quarter: int) -> pd.DataFrame:
        """获取季度杜邦分析数据。"""
        return _query(
            bs.query_dupont_data,
            f"杜邦分析 {code} {year}Q{quarter}",
            code=code,
            year=year,
            quarter=quarter,
        )

    def get_performance_express_report(
        self, code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """获取业绩快报。"""
        return _query(
            bs.query_performance_express_report,
            f"业绩快报 {code} {start_date}~{end_date}",
            code=code,
            start_date=start_date,
            end_date=end_date,
        )

    def get_forecast_report(
        self, code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """获取业绩预告。"""
        return _query(
            bs.query_forecast_report,
            f"业绩预告 {code} {start_date}~{end_date}",
            code=code,
            start_date=start_date,
            end_date=end_date,
        )

    def get_fina_indicator(
        self, code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """获取综合财务指标，聚合盈利/营运/成长/偿债/现金流/杜邦6大类数据。

        遍历日期范围内的每个季度，分别查询各类别并合并为一行。
        单个类别查询失败不影响其他类别。
        """
        description = f"综合财务指标 {code} {start_date}~{end_date}"
        logger.info(f"正在查询 {description}")

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        try:
            records = []
            for year in range(start.year, end.year + 1):
                for quarter in range(1, 5):
                    quarter_start = datetime(year, (quarter - 1) * 3 + 1, 1)
                    if quarter_start > end:
                        continue
                    record = self._build_quarter_record(code, str(year), quarter)
                    if record:
                        records.append(record)

            if not records:
                raise NoDataFoundError(f"{description}: 查询结果为空")

            df = pd.DataFrame(records)
            logger.info(f"{description}: 获取 {len(df)} 条记录")
            return df

        except (LoginError, NoDataFoundError, DataSourceError, ValueError):
            raise
        except Exception as e:
            raise DataSourceError(f"{description}: 未预期错误 - {e}") from e

    @staticmethod
    def _build_quarter_record(code: str, year: str, quarter: int) -> dict | None:
        """构建单个季度的聚合财务指标记录，无数据时返回 None。"""
        record = {"code": code, "year": year, "quarter": quarter}
        for bs_func, prefix in _FINA_CATEGORIES:
            record.update(_query_fina_category(bs_func, prefix, code, year, quarter))
        # 仅当除 code/year/quarter 外还有实际数据字段时才返回
        if len(record) <= 3:
            return None
        return record

    # ── 市场概览 ──

    def get_trade_dates(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取交易日历。"""
        return _query(
            bs.query_trade_dates,
            f"交易日历 {start_date or '默认'}~{end_date or '默认'}",
            start_date=start_date,
            end_date=end_date,
        )

    def get_all_stock(self, date: Optional[str] = None) -> pd.DataFrame:
        """获取全部股票列表及交易状态。"""
        return _query(bs.query_all_stock, f"全部股票 {date or '默认'}", day=date)

    # ── 宏观经济 ──

    def get_deposit_rate_data(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取基准存款利率。"""
        return _query(
            bs.query_deposit_rate_data,
            "基准存款利率",
            start_date=start_date,
            end_date=end_date,
        )

    def get_loan_rate_data(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取基准贷款利率。"""
        return _query(
            bs.query_loan_rate_data,
            "基准贷款利率",
            start_date=start_date,
            end_date=end_date,
        )

    def get_required_reserve_ratio_data(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        year_type: str = "0",
    ) -> pd.DataFrame:
        """获取存款准备金率。"""
        return _query(
            bs.query_required_reserve_ratio_data,
            "存款准备金率",
            start_date=start_date,
            end_date=end_date,
            yearType=year_type,
        )

    def get_money_supply_data_month(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取月度货币供应量。"""
        return _query(
            bs.query_money_supply_data_month,
            "月度货币供应量",
            start_date=start_date,
            end_date=end_date,
        )

    def get_money_supply_data_year(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取年度货币供应量。"""
        return _query(
            bs.query_money_supply_data_year,
            "年度货币供应量",
            start_date=start_date,
            end_date=end_date,
        )

    # ── 指数与行业 ──

    def get_stock_industry(
        self, code: Optional[str] = None, date: Optional[str] = None
    ) -> pd.DataFrame:
        """获取行业分类信息。"""
        return _query(
            bs.query_stock_industry, f"行业分类 {code or '全部'}", code=code, date=date
        )

    def get_sz50_stocks(self, date: Optional[str] = None) -> pd.DataFrame:
        """获取上证50成分股。"""
        return _query(bs.query_sz50_stocks, f"上证50成分股 {date or '最新'}", date=date)

    def get_hs300_stocks(self, date: Optional[str] = None) -> pd.DataFrame:
        """获取沪深300成分股。"""
        return _query(
            bs.query_hs300_stocks, f"沪深300成分股 {date or '最新'}", date=date
        )

    def get_zz500_stocks(self, date: Optional[str] = None) -> pd.DataFrame:
        """获取中证500成分股。"""
        return _query(
            bs.query_zz500_stocks, f"中证500成分股 {date or '最新'}", date=date
        )
