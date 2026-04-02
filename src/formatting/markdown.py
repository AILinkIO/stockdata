"""
数据输出格式化模块。

将 pandas DataFrame 转换为 MCP 工具返回给 LLM 的字符串格式，支持：
- markdown: Markdown 表格（默认），适合 LLM 阅读和展示
- json: JSON 格式，包含 data 数组和 meta 元信息
- csv: CSV 格式，适合结构化导出

所有格式均支持行数截断（默认 250 行），避免超长输出占用过多 token。
"""
import pandas as pd
import logging
import json

logger = logging.getLogger(__name__)

# 最大输出行数，防止返回数据过多导致 LLM 上下文溢出
MAX_MARKDOWN_ROWS = 250


def format_df_to_markdown(df: pd.DataFrame, max_rows: int = None) -> str:
    """将 DataFrame 格式化为 Markdown 表格字符串。

    超出 max_rows 限制的数据会被截断，并在输出开头添加截断提示。

    Args:
        df: 待格式化的 DataFrame
        max_rows: 最大输出行数，默认使用 MAX_MARKDOWN_ROWS

    Returns:
        Markdown 格式的表格字符串，或无数据提示
    """
    if df is None or df.empty:
        logger.warning("Attempted to format an empty DataFrame to Markdown.")
        return "(No data available to display)"

    if max_rows is None:
        max_rows = MAX_MARKDOWN_ROWS

    original_rows = df.shape[0]
    rows_to_show = min(original_rows, max_rows)
    df_display = df.head(rows_to_show)

    truncated = original_rows > rows_to_show

    try:
        markdown_table = df_display.to_markdown(index=False)
    except Exception as e:
        logger.error("Error converting DataFrame to Markdown: %s", e, exc_info=True)
        return "Error: Could not format data into Markdown table."

    if truncated:
        notes = f"rows truncated to {rows_to_show} from {original_rows}"
        return f"Note: Data truncated ({notes}).\n\n{markdown_table}"
    return markdown_table


def format_table_output(
    df: pd.DataFrame,
    format: str = "markdown",
    max_rows: int | None = None,
    meta: dict | None = None,
) -> str:
    """将 DataFrame 格式化为指定格式的字符串，支持附加元信息。

    Args:
        df: 待格式化的数据
        format: 输出格式，可选 'markdown'（默认）、'json'、'csv'
        max_rows: 最大输出行数，默认使用 MAX_MARKDOWN_ROWS
        meta: 可选的元信息字典，markdown 格式下作为头部输出，json 格式下嵌入 meta 字段

    Returns:
        适合作为 MCP 工具返回值的格式化字符串
    """
    fmt = (format or "markdown").lower()

    # Normalize row cap
    if max_rows is None:
        max_rows = MAX_MARKDOWN_ROWS if fmt == "markdown" else MAX_MARKDOWN_ROWS

    total_rows = 0 if df is None else int(df.shape[0])
    rows_to_show = 0 if df is None else min(total_rows, max_rows)
    truncated = total_rows > rows_to_show
    df_display = df.head(rows_to_show) if df is not None else pd.DataFrame()

    if fmt == "markdown":
        header = ""
        if meta:
            # Render a compact meta header
            lines = ["Meta:"]
            for k, v in meta.items():
                lines.append(f"- {k}: {v}")
            header = "\n".join(lines) + "\n\n"
        return header + format_df_to_markdown(df_display, max_rows=max_rows)

    if fmt == "csv":
        try:
            return df_display.to_csv(index=False)
        except Exception as e:
            logger.error("Error converting DataFrame to CSV: %s", e, exc_info=True)
            return "Error: Could not format data into CSV."

    if fmt == "json":
        try:
            payload = {
                "data": [] if df_display is None else df_display.to_dict(orient="records"),
                "meta": {
                    **(meta or {}),
                    "total_rows": total_rows,
                    "returned_rows": rows_to_show,
                    "truncated": truncated,
                    "columns": [] if df_display is None else list(df_display.columns),
                },
            }
            return json.dumps(payload, ensure_ascii=False)
        except Exception as e:
            logger.error("Error converting DataFrame to JSON: %s", e, exc_info=True)
            return "Error: Could not format data into JSON."

    # Fallback to markdown if unknown format
    logger.warning("Unknown format '%s', falling back to markdown", fmt)
    return format_df_to_markdown(df_display, max_rows=max_rows)
