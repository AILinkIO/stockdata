# stockdata 数据面 API（/api/v1）

供下游拉取数据的只读 RESTful API。所有数据来自本地 PostgreSQL（由同步任务灌入），
接口本身**绝不触碰 baostock**，可任意频率调用。

- **Base URL**：`http://<host>:8050`
- **在线文档**：`/docs`（Swagger UI）、`/openapi.json`
- 交互式控制面（启动/停止同步）见 `/api/sync/*`，不在本文档范围。

## 通用约定

**响应信封**——所有端点统一：

```json
{ "data": <载荷>, "meta": { "count": 12, ... } }
```

**错误**——FastAPI 标准格式 + 状态码：

```json
{ "detail": "证券不存在：sh.999999" }
```

| 状态码 | 含义 |
|---|---|
| 200 | 成功（查不到数据返回空 `data`，不是 404） |
| 401 | API Key 缺失/错误（仅在鉴权开启时） |
| 404 | 资源不存在（仅 `securities/{code}`） |
| 422 | 参数不合法（枚举越界、批量超 500 码等） |

**鉴权**——环境变量 `STOCKDATA_API_KEY` 为空（默认）不鉴权；配置后所有
`/api/v1/*` 请求必须带头 `X-API-Key: <key>`。

**格式**——日期 `YYYY-MM-DD`；时间戳 ISO-8601 带时区（分钟线 `bar_time` 为上海时间）；
数值列一律输出 JSON number；财报指标/分红明细为对象（键为 baostock 原始字段名）。

**批量端点**——`POST` + JSON body，`codes` 数组 ≤ 500；`data` 为按 code 分组的字典，
库里没有的 code 对应空数组（不报错）。

**增量拉取推荐流程**——先查水位再拉数，避免把未同步误当成无数据：

```bash
curl "$BASE/api/v1/meta/watermarks?code=sh.600050&dataset=k_d"
#   → last_date=2026-07-16 说明日K已覆盖到 7-16
curl "$BASE/api/v1/kline/sh.600050?freq=d&start=2026-07-10"
```

---

## 行情

### GET /api/v1/kline/{code}

K 线（服务端读时复权）。

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `freq` | `5`/`30`/`d`/`w` | `d` | 频率：5分/30分/日/周 |
| `start`/`end` | date | 不限 | 闭区间过滤 |
| `adjust` | `none`/`fore`/`back` | `none` | 复权：不/前/后（back 因子 as-of 推导） |
| `limit` | int ≤100000 | 10000 | 升序截断；`meta.truncated=true` 表示被截断 |

```bash
curl "$BASE/api/v1/kline/sh.600050?freq=d&start=2026-07-15&adjust=back"
```

```json
{
  "data": [
    { "trade_date": "2026-07-16", "open": 4.25, "high": 4.29, "low": 4.24,
      "close": 4.28, "volume": 147594413, "amount": 629623075.93,
      "turn": 0.4763, "pct_chg": 0.2342, "preclose": 4.27,
      "trade_status": 1, "is_st": false,
      "pe_ttm": 15.455394, "pb_mrq": 0.783881, "ps_ttm": 0.341624,
      "pcf_ncf_ttm": 162141.805165 }
  ],
  "meta": { "code": "sh.600050", "freq": "d", "adjust": "back",
            "count": 2, "truncated": false }
}
```

分钟线（`freq=5/30`）行结构为：`bar_time, open, high, low, close, volume, amount`。

### POST /api/v1/kline/batch

```json
{ "codes": ["sh.600050", "sz.000001"], "freq": "d",
  "start": "2026-07-01", "end": null, "adjust": "none", "limit_per_code": 5000 }
```

响应 `data` 按 code 分组；`meta.truncated` 为被截断的 code 列表。

### GET /api/v1/adjust-factors/{code} · POST /api/v1/adjust-factors/batch

复权因子（除权除息日事件表）。行结构：

```json
{ "divid_operate_date": "2025-06-10", "fore_adjust_factor": 1.0,
  "back_adjust_factor": 1.25, "adjust_factor": 1.25 }
```

batch body：`{ "codes": [...] }`。

---

## 参考数据

### GET /api/v1/securities

| 参数 | 说明 |
|---|---|
| `type` | 1 股票 / 2 指数 / 3 其他 |
| `status` | 1 上市 / 0 退市 |
| `q` | 代码/名称模糊搜索 |
| `limit`（≤1000，默认 100）/ `offset` | 分页；`meta.total` 为总数 |

### GET /api/v1/securities/{code}

单票详情（404 语义）。含最新行业快照：

```json
{ "data": { "code": "sh.600050", "code_name": "中国联通", "ipo_date": "2002-10-09",
            "out_date": null, "type": 1, "status": 1,
            "industry": "…", "industry_classification": "…",
            "industry_snap_date": "2026-07-14" } }
```

行业三字段来自 `industry` 数据集的最新快照，未同步过时为 `null`。

### GET /api/v1/trade-calendar

`start` / `end` / `only_trading=true`（只返回交易日）。
行：`{ "calendar_date": "2026-07-16", "is_trading_day": true }`。

### GET /api/v1/industries

行业分类快照；`date` 缺省取最新一期（实际快照日期见 `meta.snap_date`）。
行：`{ "code", "industry", "industry_classification" }`。

### GET /api/v1/index-constituents/{index}

`index` ∈ `sz50` / `hs300` / `zz500`；`date` 缺省最新。
行：`{ "code", "code_name" }`；`meta.snap_date` 为快照日期。

---

## 财务 / 事件

### GET /api/v1/financials/{code}

| 参数 | 说明 |
|---|---|
| `type` | `profit` 盈利 / `operation` 营运 / `growth` 成长 / `balance` 偿债 / `cash_flow` 现金流 / `dupont` 杜邦 / `performance_express` 业绩快报 / `forecast` 业绩预告 |
| `start`/`end` | 按报告期（stat_date）过滤 |

```json
{ "data": [ { "stat_date": "2026-03-31", "pub_date": "2026-04-20",
              "metrics": { "roeAvg": "0.1", "npMargin": "0.08", "...": "..." } } ],
  "meta": { "code": "sh.600050", "type": "profit", "count": 1 } }
```

`metrics` 键为 baostock 原始指标名，值为原始字符串（保精度）。

### POST /api/v1/financials/batch

body：`{ "codes": [...], "type": "profit", "start": null, "end": null }`。

### GET /api/v1/dividends/{code}

`year` 按预案公告年过滤。行：

```json
{ "plan_announce_date": "2025-05-01", "year_type": "operate",
  "operate_date": "2025-06-10", "detail": { "dividCashPsBeforeTax": "0.25", "...": "..." } }
```

### POST /api/v1/dividends/batch

body：`{ "codes": [...], "year": 2025 }`。

---

## 宏观 / 元数据

### GET /api/v1/macro/{kind}

`kind` ∈ `deposit_rate` 存款利率 / `loan_rate` 贷款利率 / `rrr` 准备金率 /
`money_supply_month` 货币供应(月) / `money_supply_year` 货币供应(年)。
`start`/`end` 按 `date_key` 文本序过滤（`date_key` 为 `YYYY-MM-DD` / `YYYY-MM` / `YYYY`，
视 kind 而定），其余字段为 baostock 原始载荷平铺。

### GET /api/v1/meta/watermarks

数据新鲜度：每 (code, dataset) 一行，市场级数据集 `code=""`。

| 参数 | 说明 |
|---|---|
| `code` | 过滤单票（`""` 可查市场级） |
| `dataset` | 如 `k_d`/`k_5`/`financial`/`trade_calendar`… |
| `limit`（≤10000，默认 1000）/ `offset` | 分页；`meta.total` 总数 |

```json
{ "data": [ { "code": "sh.600050", "dataset": "k_d",
              "first_date": "2002-10-09", "last_date": "2026-07-16",
              "last_synced_at": "2026-07-17T13:26:25+08:00" } ],
  "meta": { "total": 1, "count": 1, "limit": 1000, "offset": 0 } }
```

语义：`[first_date, last_date]` 为已覆盖区间；`last_date` 只推进到「已结算边界」
（日线=昨天、周线=上一收盘周五等），未结算尾部绝不虚报。
