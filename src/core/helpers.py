"""
代码标准化模块。

提供股票代码和指数代码的格式标准化函数，将各种常见输入格式
统一转换为 Baostock 格式（如 'sh.600000'、'sh.000300'）。

支持的股票代码输入格式：
- Baostock 格式: 'sh.600000', 'sz.000001'
- 无分隔符:      'sh600000', 'SZ000001'
- 后缀格式:      '600000.SH', '000001.sz'
- 纯数字:        '600000'（6开头自动识别为上海）, '000001'（其余为深圳）

支持的指数代码输入格式：
- 数字代码: '000300', '000016', '000905'
- 英文别名: 'CSI300', 'HS300', 'SSE50', 'SZ50', 'ZZ500', 'CSI500'
"""
import re

from src.services.validation import validate_non_empty_str


def normalize_stock_code_logic(code: str) -> str:
    """将股票代码标准化为 Baostock 格式 'xx.xxxxxx'。

    Args:
        code: 任意常见格式的股票代码

    Returns:
        Baostock 格式的股票代码，如 'sh.600000'

    Raises:
        ValueError: 无法识别的代码格式
    """
    validate_non_empty_str(code, "code")
    raw = code.strip()

    # 格式: sh.600000 / SH600000 / sh600000
    m = re.fullmatch(r"(?i)(sh|sz)[.]?(\d{6})", raw)
    if m:
        ex, num = m.group(1).lower(), m.group(2)
        return f"{ex}.{num}"

    # 格式: 600000.SH / 000001.sz
    m2 = re.fullmatch(r"(\d{6})[.]?(?i:(sh|sz))", raw)
    if m2:
        num, ex = m2.group(1), m2.group(2).lower()
        return f"{ex}.{num}"

    # 格式: 纯6位数字，按首位判断交易所
    m3 = re.fullmatch(r"(\d{6})", raw)
    if m3:
        num = m3.group(1)
        ex = "sh" if num.startswith("6") else "sz"
        return f"{ex}.{num}"

    raise ValueError("Unsupported code format. Examples: 'sh.600000', '600000', '000001.SZ'.")


def normalize_index_code_logic(code: str) -> str:
    """将指数代码标准化为 Baostock 格式。

    Args:
        code: 指数代码或英文别名

    Returns:
        Baostock 格式的指数代码，如 'sh.000300'

    Raises:
        ValueError: 无法识别的指数代码
    """
    validate_non_empty_str(code, "code")
    raw = code.strip().upper()
    if raw in {"000300", "CSI300", "HS300"}:
        return "sh.000300"
    if raw in {"000016", "SSE50", "SZ50"}:
        return "sh.000016"
    if raw in {"000905", "ZZ500", "CSI500"}:
        return "sh.000905"
    raise ValueError("Unsupported index code. Examples: 000300/CSI300/HS300, 000016, 000905.")
