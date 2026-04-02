"""
MCP 工具统一错误处理模块。

所有 MCP 工具通过 run_tool_with_handling() 执行业务逻辑，
该函数将各类异常统一转换为用户友好的错误字符串返回给 LLM，
避免异常直接暴露到 MCP 协议层。

异常处理优先级（从具体到通用）：
1. NoDataFoundError  — 查询无数据，通常是参数范围问题
2. LoginError        — 数据源登录失败
3. DataSourceError   — 数据源层面的其他错误
4. ValueError        — 输入参数校验失败
5. Exception         — 兜底捕获所有未预期异常
"""
import logging
from typing import Callable

from src.providers.interface import NoDataFoundError, LoginError, DataSourceError

logger = logging.getLogger(__name__)


def run_tool_with_handling(action: Callable[[], str], context: str) -> str:
    """执行工具的业务逻辑并将异常统一转换为错误字符串。

    Args:
        action: 无参可调用对象，返回格式化后的字符串结果。
        context: 日志上下文标识（如 "get_historical_k_data:sh.600000"），用于定位问题。

    Returns:
        业务逻辑的正常返回值，或以 "Error: " 开头的错误描述字符串。
    """
    try:
        return action()
    except NoDataFoundError as e:
        logger.warning(f"{context}: 未找到数据: {e}")
        return f"Error: {e}"
    except LoginError as e:
        logger.error(f"{context}: 登录错误: {e}")
        return f"Error: Could not connect to data source. {e}"
    except DataSourceError as e:
        logger.error(f"{context}: 数据源错误: {e}")
        return f"Error: An error occurred while fetching data. {e}"
    except ValueError as e:
        logger.warning(f"{context}: 参数校验错误: {e}")
        return f"Error: Invalid input parameter. {e}"
    except Exception as e:
        logger.exception(f"{context}: 未预期错误: {e}")
        return f"Error: An unexpected error occurred: {e}"
