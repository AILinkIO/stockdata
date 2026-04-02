"""
工具输入参数校验模块。

集中定义各工具参数的合法值，并提供统一的校验函数。
校验失败时抛出 ValueError，由 run_tool_with_handling 统一捕获并返回错误信息。
"""
from typing import Iterable

# K线数据频率: d=日, w=周, m=月, 5/15/30/60=分钟
VALID_FREQS = ["d", "w", "m", "5", "15", "30", "60"]
# 复权类型: 1=后复权, 2=前复权, 3=不复权
VALID_ADJUST_FLAGS = ["1", "2", "3"]
# 输出格式
VALID_FORMATS = ["markdown", "json", "csv"]
# 分红年份类型: report=预案公告年份, operate=除权除息年份
VALID_YEAR_TYPES = ["report", "operate"]
# 存款准备金率年份类型: 0=全部, 1=大型, 2=中小型
VALID_RESERVE_YEAR_TYPES = ["0", "1", "2"]


def _ensure_in(value: str, allowed: Iterable[str], label: str) -> None:
    if value not in allowed:
        raise ValueError(f"Invalid {label} '{value}'. Valid options are: {list(allowed)}")


def validate_frequency(frequency: str) -> None:
    _ensure_in(frequency, VALID_FREQS, "frequency")


def validate_adjust_flag(adjust_flag: str) -> None:
    _ensure_in(adjust_flag, VALID_ADJUST_FLAGS, "adjust_flag")


def validate_output_format(fmt: str) -> None:
    _ensure_in(fmt, VALID_FORMATS, "format")


def validate_year(year: str) -> None:
    if not year.isdigit() or len(year) != 4:
        raise ValueError(f"Invalid year '{year}'. Please provide a 4-digit year.")


def validate_year_type(year_type: str) -> None:
    _ensure_in(year_type, VALID_YEAR_TYPES, "year_type")


def validate_quarter(quarter: int) -> None:
    if quarter not in (1, 2, 3, 4):
        raise ValueError("Invalid quarter. Must be between 1 and 4.")


def validate_non_empty_str(value: str, label: str) -> None:
    if value is None or not str(value).strip():
        raise ValueError(f"'{label}' is required.")


def validate_index_key(value: str, mapping: dict) -> str:
    key = mapping.get(value.lower()) if isinstance(value, str) else None
    if not key:
        raise ValueError(f"Invalid index '{value}'. Valid options: {sorted(set(mapping.values()))}")
    return key


def validate_year_type_reserve(year_type: str) -> None:
    _ensure_in(year_type, VALID_RESERVE_YEAR_TYPES, "year_type")


def validate_limit(limit: int) -> None:
    if limit <= 0:
        raise ValueError("limit must be positive.")
