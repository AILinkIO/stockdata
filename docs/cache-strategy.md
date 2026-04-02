# 数据缓存策略设计

## 背景

A 股市场数据一旦公布，大部分不会再变化，适合缓存以减少对 Baostock API 的重复调用。
但部分场景存在数据追溯调整（如复权K线）或尚未定稿（如当季财报），需要区分处理。

使用 diskcache（SQLite 后端）作为缓存存储，持久化到磁盘，重启服务不丢失。

## TTL 常量

| 常量 | 值 | 用途 |
|---|---|---|
| `PERMANENT` | 7776000（90 天兜底） | 历史确定性数据 |
| `TTL_REALTIME` | 300（5 分钟） | 盘中可能变化的数据 |
| `TTL_DAILY` | 86400（1 天） | 日级别变化频率的数据 |
| `TTL_WEEKLY` | 604800（7 天） | 低频变化的数据 |

## 各方法缓存策略

### K 线数据 `get_historical_k_data`

#### 不复权（adjust_flag="3"）—— 按月拆分缓存

将查询结果按自然月拆分，每个月独立缓存。不同日期范围的查询可复用已缓存的月份，
仅需补查缺失的月份。

**流程**：

1. 根据 `start_date ~ end_date` 生成月份列表 `[(year, month), ...]`
2. 逐月检查缓存，区分已命中 / 缺失
3. 将连续缺失月份合并为一次 API 调用（最少请求数）
4. API 返回后按 `date` 列的月份拆分，各月独立写入缓存
5. 按原始请求的 `start_date ~ end_date` 过滤后返回

**缓存键**：`("k_month", code, year, month, frequency, adjust_flag, fields)`

**月份 TTL**（辅助函数 `_is_month_completed`）：

| 频率 | 月份已完成条件 | TTL |
|---|---|---|
| `d` / `5` / `15` / `30` / `60` | 月末 < 今天 | 永久（90天兜底） |
| `w` | 月末 < 本周一 | 永久（90天兜底） |
| `m` | 月末 < 本月1日 | 永久（90天兜底） |
| 当前月份（未完成） | — | 5 分钟 |

**示例**：查询 `2025-10-01 ~ 2026-04-02`（今天），首次拆分为 7 个月份块，
一次 API 调用获取全部数据；10月~3月永久缓存，4月 5 分钟 TTL。
之后查询 `2025-11-01 ~ 2026-04-02` 时，11月~3月直接命中，仅需拉取 4 月。

#### 复权（adjust_flag="1"/"2"）

不使用短 TTL 盲猜，而是将复权因子的 fingerprint 嵌入缓存 key：

1. 查复权 K 线前，先查该股复权因子（本身走缓存，end_date < 今天时永久缓存）
2. 从复权因子 DataFrame 计算 fingerprint：`(行数, 末行日期, 末行因子值)`
3. 将 fingerprint 嵌入缓存 key
4. 复权因子没变 → key 命中 → 返回缓存（等效永久缓存）
5. 发生新除权 → 复权因子变了 → fingerprint 不同 → cache miss → 重新拉取

**旧缓存清理**：fingerprint 变化后旧 key 对应的缓存成为孤儿数据，
通过对复权 K 线设置一个较长的 TTL 上限（如 30 天）作为兜底清理，避免磁盘膨胀。

### 基本信息 `get_stock_basic_info`

| 条件 | TTL | 原因 |
|---|---|---|
| 任何情况 | 1 天 | 偶有名称/行业变更，日级别 TTL 足够 |

### 分红数据 `get_dividend_data`

| 条件 | TTL | 原因 |
|---|---|---|
| `year < 当前年份` | 永久 | 往年方案已全部实施 |
| `year >= 当前年份` | 1 天 | 可能有新预案公布或方案调整 |

### 复权因子 `get_adjust_factor_data`

| 条件 | TTL | 原因 |
|---|---|---|
| `end_date < 今天` | 永久 | 历史因子已固定 |
| `end_date >= 今天` | 5 分钟 | 今天可能发生除权 |

此方法同时承担两个角色：
1. 直接返回复权因子给调用方
2. 作为复权 K 线缓存失效的判据（计算 fingerprint）

### 季度财务报表（6 个方法）

`get_profit_data` / `get_operation_data` / `get_growth_data` / `get_balance_data` / `get_cash_flow_data` / `get_dupont_data`

| 条件 | TTL | 原因 |
|---|---|---|
| 季度报告截止日已过 | 永久 | 发布后不变 |
| 截止日未到 | 1 天 | 可能尚未发布或有修正 |

**季度报告截止日**（辅助函数 `_is_past_quarter`）：

| 季度 | 披露截止日 |
|---|---|
| Q1 | 当年 4 月 30 日 |
| Q2 | 当年 8 月 31 日 |
| Q3 | 当年 10 月 31 日 |
| Q4 | 次年 4 月 30 日 |

### 业绩快报 / 业绩预告 `get_performance_express_report` / `get_forecast_report`

| 条件 | TTL | 原因 |
|---|---|---|
| `end_date < 今天` | 永久 | 公告后不变 |
| 包含今天 | 1 天 | 可能有新公告 |

### 综合财务指标 `get_fina_indicator`

| 条件 | TTL | 原因 |
|---|---|---|
| `end_date` 年份 < 当前年份 | 永久 | 聚合的底层数据已固定 |
| 当前年份 | 1 天 | 当年季报可能更新 |

### 交易日历 `get_trade_dates`

| 条件 | TTL | 原因 |
|---|---|---|
| `end_date < 今天` | 永久 | 历史日历不变 |
| 包含今天或未来 | 1 天 | 偶有临时调整 |

### 全部股票列表 `get_all_stock`

| 条件 | TTL | 原因 |
|---|---|---|
| `date < 今天` | 永久 | 历史状态不变 |
| `date` 为今天或 None | 1 天 | 新股上市/退市 |

### 宏观经济数据（5 个方法）

`get_deposit_rate_data` / `get_loan_rate_data` / `get_required_reserve_ratio_data` / `get_money_supply_data_month` / `get_money_supply_data_year`

| 条件 | TTL | 原因 |
|---|---|---|
| `end_date < 2 个月前` | 永久 | 宏观数据发布后不变 |
| `end_date` 在近 2 个月内或 None | 7 天 | 数据发布有 1-2 个月滞后，近期可能尚未完整 |

### 行业分类 `get_stock_industry`

| 条件 | TTL | 原因 |
|---|---|---|
| `date < 今天` | 永久 | 历史分类不变 |
| `date` 为 None 或今天 | 7 天 | 偶有重新分类 |

### 指数成分股 `get_hs300_stocks` / `get_sz50_stocks` / `get_zz500_stocks`

| 条件 | TTL | 原因 |
|---|---|---|
| `date < 今天` | 永久 | 历史成分不变 |
| `date` 为 None 或今天 | 1 天 | 季度调整 |

## 辅助函数清单

| 函数 | 职责 |
|---|---|
| `_is_month_completed(year, month, frequency)` | 判断指定月份的 K 线数据是否已定型 |
| `_generate_months(start_date, end_date)` | 生成日期范围内所有 (year, month) 元组 |
| `_month_date_range(year, month)` | 返回月份的首日/末日日期字符串 |
| `_group_contiguous_months(months)` | 将连续月份分组，用于合并 API 调用 |
| `_is_past_quarter(year, quarter)` | 判断该季度财报是否已过披露截止日 |
| `_is_past_date(date_str)` | 判断日期是否在今天之前 |
| `_is_macro_settled(end_date)` | 判断宏观数据日期是否超过 2 个月前 |
| `_compute_adjust_fingerprint(adj_df)` | 从复权因子 DataFrame 计算 `(行数, 末行日期, 末行因子值)` |
| `_make_key(method_name, **kwargs)` | 构建缓存 key |

## 架构

缓存层采用装饰器模式，在数据源层（DataFrame 级别）缓存，对工具层完全透明：

```
工具层（tools/）
  ↓ 调用
数据源单例（data_source.py）
  = CachedDataSource（缓存代理）
    ↓ 缓存未命中时调用
    BaostockDataSource（实际 API 调用）
```

缓存目录：项目根目录下 `.cache/stockdata/`（已在 .gitignore 中排除）。
