# 日线 k_d 端到端迁移方案（P3/P4 切片）

> 配套 `TASK.md`。这是迁移的第一条端到端竖切：把日线 K（`k_d`）从"Python 全包"切到
> "Python 抓取微服务 + dotnet 数据属主"。打通后按此模板扩到其余数据类型（P5）。
> 已定决策：D-A 异步 submit+poll、D-B EF Core+LINQ、D-C TTL 600s/1200s、D-D 重建库。

## 0. 锁定常量（沿用现有久经考验的值）

| 常量 | 值 | 来源 |
|------|----|----|
| `k_d` 刷新间隔 | 300s | `coverage.REFRESH_INTERVALS["k_d"]` |
| `k_d` 定型边界 | `today - 1d` | `coverage.settled_boundary` 默认分支 |
| `k_d` 回填起点 | `1990-12-19`（上交所开市） | `coverage._A_SHARE_EPOCH` |
| `k_d` 切片上限 | 3650 天/段 | `readthrough._SLICE_DAYS["k_d"]` |
| 完成态 job TTL | 600s | 旧 `result_expires` |
| 在途/僵尸阈值 | 1200s | 旧 `_STALE_RUNNING` |
| 轮询间隔 | 0.5s | 旧 `_POLL_INTERVAL` |
| 等待超时 | `fetch_wait_timeout` | 沿用 |
| 限流 | 60/min, redis db4 | 已配 |
| 退避 | base 30 ×2^n 封顶 180，最多 8 次 | `settings.fetch_*` |

## 1. 数据流（k_d 读穿透）

```
MCP get_historical_k_data(frequency=d)
  └─► dotnet KlineService.EnsureRange(code, k_d, start, end)
        1. wm = PG.SELECT data_watermark(code, "k_d")            ← EF Core
        2. decision = Coverage.CheckRange(wm, "k_d", start, end, now)   ← 移植
        3. for (fs,fe) in decision.FetchRanges:
             for (ss,se) in Slice(fs,fe, 3650):                  ← 切片在 dotnet
               jobId = POST /fetch {type:"fetch_kline", params:{code,ss,se,frequency:"d"}}
               payload = PollUntilDone(jobId, timeout)           ← GET /fetch/{id}
               using tx = Db.BeginTransaction():
                 UpsertKline(payload)                            ← ON CONFLICT (ExecuteSqlRaw)
                 UpdateWatermark("k_d", code,
                    last_date = ClaimableLast("k_d", se, MaxDate(payload), today),
                    first_date = ss)                             ← GREATEST/LEAST upsert
               tx.Commit()
  └─► dotnet 直读 PG: SELECT kline WHERE code,frequency='d',trade_date∈[start,end]
        └─► 返回 JSON 给 MCP 工具
```

Python 全程**不碰 PG、不判 coverage、不切片**——只对"给定区间"抓一次 baostock 并把原始结果丢回 Redis。

## 2. Python 抓取微服务（k_d）

### 2.1 `POST /fetch` — 提交（异步，D-A）

请求：
```json
{ "type": "fetch_kline", "params": { "code": "sh.600000",
  "start_date": "2024-01-01", "end_date": "2024-03-31", "frequency": "d" } }
```
处理：
1. `params_hash = sha256(sort_keys(type+params))`（沿用 `_params_hash` 算法）。
2. `SETNX job:idx:{params_hash} = job_id`：
   - 已存在 → 取既有 job_id，返回 `{job_id, status, dedup:true}`（**在飞搭车 / 读穿透与 beat 去重**）。
   - 不存在 → 新建 `job:{job_id}`（status=pending），入内部队列。
3. 返回 `202 {job_id, status:"pending", dedup:false}`。**绝不阻塞**。

### 2.2 `GET /fetch/{job_id}` — 查询

```json
{ "job_id":"...", "status":"done",
  "meta": { "rows": 59 },
  "payload": { "fields": ["date","code","open",...,"isST"],
               "rows": [ ["2024-01-02","sh.600000","10.20",...], ... ] },
  "error": null }
```
- `status ∈ {pending, running, done, failed}`。
- `payload` 仅 `done` 时有：**baostock 原样字符串**（fields + rows，不解释类型）。schema 知识全在 dotnet（单向契约）。
- `failed` 时 `error` 带消息（退避耗尽后的 DataSourceError 文案）。

### 2.3 Redis job 模型与状态机

| key | 类型 | 内容 | TTL |
|-----|------|------|-----|
| `job:{id}` | hash | type, params, params_hash, status, started_at, finished_at, error, rows | 在途 1200s → 完成 600s |
| `job:result:{id}` | string | payload JSON（独立 key，便于大 payload 单独过期） | 完成 600s |
| `job:idx:{params_hash}` | string | → job_id（去重索引） | 跟随 job：在途 1200s / 完成 600s |

状态机：`pending → running → done│failed`。
- 进 running 写 `started_at` + 心跳刷新 1200s TTL（防长回填段被误判僵尸）。
- 到 done/failed：写 payload/error，把 `job:*` 与 `idx` 的 TTL 统一压到 600s。
- 1200s 内无心跳（worker 崩）→ key 过期 → dedup 释放，dotnet 下次 POST 重建 job（幂等）。

### 2.4 内部 worker（限流 + 长会话）

- **单消费者串行**（baostock 全局单连接约束，等同旧 solo pool）。
- 取 job → **限流闸（redis db4）** → `provider.query_k_data(code,ss,se,"d")` → 写 `job:result` + status=done。
- 出错走**退避重试**（沿用 `_backoff_seconds`，最多 8 次）；耗尽 → status=failed。
- **长生命周期单例 baostock 会话，出错才 `force_relogin`，进程不频繁重启**（TASK §0 红线）。
- `NoDataFoundError`（合法空结果）→ status=done, `payload.rows=[]`（dotnet 据此走"空结果"水位规则）。

## 3. dotnet 侧（k_d）

### 3.1 EF Core 实体（精确镜像现有 SQLAlchemy 模型）

```csharp
[PrimaryKey(nameof(Code), nameof(Frequency), nameof(TradeDate))]
public class Kline {
    public string Code { get; set; }          // varchar(12)
    public char   Frequency { get; set; }      // char(1): 'd'
    public DateOnly TradeDate { get; set; }     // date
    [Precision(12,4)] public decimal? Open { get; set; }
    [Precision(12,4)] public decimal? High { get; set; }
    [Precision(12,4)] public decimal? Low { get; set; }
    [Precision(12,4)] public decimal? Close { get; set; }
    [Precision(12,4)] public decimal? Preclose { get; set; }
    public long?   Volume { get; set; }         // bigint
    [Precision(20,4)] public decimal? Amount { get; set; }
    [Precision(10,6)] public decimal? Turn { get; set; }
    [Precision(10,6)] public decimal? PctChg { get; set; }
    public short?  TradeStatus { get; set; }    // smallint
    public bool?   IsSt { get; set; }
    [Precision(14,6)] public decimal? PeTtm { get; set; }
    [Precision(14,6)] public decimal? PbMrq { get; set; }
    [Precision(14,6)] public decimal? PsTtm { get; set; }
    [Precision(14,6)] public decimal? PcfNcfTtm { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
}

[PrimaryKey(nameof(Code), nameof(DataType))]
public class DataWatermark {
    public string Code { get; set; }            // varchar(12), 全市场用 ""
    public string DataType { get; set; }        // varchar(24): "k_d"
    public DateOnly? FirstDate { get; set; }
    public DateOnly  LastDate { get; set; }
    public DateTimeOffset LastFetchedAt { get; set; }
}
```

### 3.2 `Coverage.CheckRange` 移植（k_d 路径）

```csharp
public record Decision(IReadOnlyList<(DateOnly Start, DateOnly End)> FetchRanges, string Reason) {
    public bool Fresh => FetchRanges.Count == 0;
}

public static Decision CheckRange(
    DataWatermark? wm, string dataType, DateOnly start, DateOnly end, DateTimeOffset now)
{
    var today = TodayCst(now);
    // ① 钳制未来（k_d != trade_calendar）
    if (end > today) end = today;
    if (end < start) return new([], "请求范围全部在未来");

    // ② 首次触达：全量回填
    if (wm is null) {
        var from = Min(BackfillStart(dataType), start);   // k_d → 1990-12-19
        return new([(from, end)], "首次触达，全量回填");
    }

    var ranges = new List<(DateOnly,DateOnly)>();
    var boundary = SettledBoundary(dataType, today);       // k_d → today-1
    var stale = (now - wm.LastFetchedAt).TotalSeconds > RefreshInterval(dataType); // 300

    // ③ 头部缺口
    if (wm.FirstDate is {} fd && start < fd)
        ranges.Add((start, fd.AddDays(-1)));

    // ④ 尾部缺口（未定型尾部节流）
    if (end > wm.LastDate) {
        bool gapUnsettledOnly = dataType != "trade_calendar" && wm.LastDate >= boundary;
        if (!gapUnsettledOnly || stale)
            ranges.Add((wm.LastDate.AddDays(1), end));
    }

    // ⑤ 未定型区刷新：起点取"上次抓取时"的定型边界+1
    if (stale) {
        var fetchedDay = ToCstDate(wm.LastFetchedAt);
        var refreshFrom = Max(start, SettledBoundary(dataType, fetchedDay).AddDays(1));
        // 注：k_d 时 SettledBoundary(fetchedDay)+1 == fetchedDay，即从上次抓取日重刷
        var refreshTo = dataType == "trade_calendar" ? end : Min(end, today);
        if (refreshFrom <= refreshTo) ranges.Add((refreshFrom, refreshTo));
    }

    return ranges.Count == 0
        ? new([], "已覆盖且新鲜")
        : new(MergeRanges(ranges), "存在缺口或未定型数据过期");
}
```
`MergeRanges` = 排序后相邻（≤1 天间隔）/重叠合并（沿用 `_merge_ranges`）。

### 3.3 `ClaimableLast` 移植（写侧水位规则）

```csharp
public static DateOnly ClaimableLast(string dataType, DateOnly requestedEnd,
                                     DateOnly? actualLast, DateOnly today) {
    var claimed = Min(requestedEnd, SettledBoundary(dataType, today));
    if (actualLast is {} a && a > claimed) claimed = a;
    return claimed;
}
```
`actualLast = MaxDate(payload)` = payload.rows 中 `date` 列最大值（空结果 → null）。

### 3.4 落盘（数据 + 水位同事务，ExecuteSqlRaw ON CONFLICT）

```csharp
using var tx = await db.Database.BeginTransactionAsync();
// kline 批量 upsert（分块，参数化 ON CONFLICT）
await db.Database.ExecuteSqlRawAsync(@"
  INSERT INTO kline (code,frequency,trade_date,open,high,low,close,preclose,
     volume,amount,turn,pct_chg,trade_status,is_st,pe_ttm,pb_mrq,ps_ttm,pcf_ncf_ttm,updated_at)
  SELECT * FROM unnest(@rows...) 
  ON CONFLICT (code,frequency,trade_date) DO UPDATE SET
     open=EXCLUDED.open, ..., pcf_ncf_ttm=EXCLUDED.pcf_ncf_ttm, updated_at=now()", rows);
// 水位 upsert：first 取更早、last 取更晚（NULL 安全）
await db.Database.ExecuteSqlRawAsync(@"
  INSERT INTO data_watermark (code,data_type,first_date,last_date,last_fetched_at)
  VALUES (@code,'k_d',@first,@last,now())
  ON CONFLICT (code,data_type) DO UPDATE SET
     first_date=LEAST(data_watermark.first_date, EXCLUDED.first_date),
     last_date =GREATEST(data_watermark.last_date, EXCLUDED.last_date),
     last_fetched_at=now()", ...);
await tx.CommitAsync();
```
**同事务**保证 SIGKILL 时数据与水位全提交/全回滚（复刻 `SyncSession.begin()` 语义）。

### 3.5 字段映射表（baostock → 列 → 类型，逐字段对照 writer `_K_COL_MAP`）

| baostock | 列 | 转换 | 备注 |
|----------|----|----|----|
| date | trade_date | date | |
| open/high/low/close/preclose | 同名 | decimal | 空串→null |
| volume | volume | long | 空串→null |
| amount | amount | decimal | |
| turn | turn | decimal | |
| pctChg | pct_chg | decimal | |
| tradestatus | trade_status | short | 仅日线 |
| isST | is_st | bool | `=="1"` |
| peTTM/pbMRQ/psTTM/pcfNcfTTM | pe_ttm/pb_mrq/ps_ttm/pcf_ncf_ttm | decimal | 仅日线估值四件套 |

转换规则（复刻 writer）：**空串/None → null；decimal 用字符串构造绝不经 double/float；
解析失败 → null（不抛）**。`code`、`frequency='d'` 由 dotnet 注入（payload 的 code 列冗余，忽略）。

## 4. 崩溃安全与失效模式

| 场景 | 处理 |
|------|------|
| Python 抓成功、dotnet 落盘前崩 | job result 在 Redis 留 600s；dotnet 重启后重 POST → dedup 命中 → 取缓存 payload 落盘。超 600s → 重抓（幂等 upsert，浪费一次额度，可接受） |
| dotnet 落盘中途崩 | 同事务回滚，无半写；水位未推进，下次 CheckRange 重新判出同缺口 |
| 同 (code,range) 并发（读穿透 + beat） | Python `params_hash` 去重 → 同 job_id，两方都轮询同结果，只抓一次 |
| baostock 空结果（停牌/未发布） | payload.rows=[] → ClaimableLast 只在定型区推进水位（声明"查过没有"），未定型尾部不虚假声明（防永久空洞） |
| Python worker 崩（job 卡 running） | 1200s 无心跳 → key 过期 → dedup 释放 → dotnet 重 POST 重建 |

## 5. 测试对照清单（黄金标准：`test_coverage` + `test_readthrough_slicing`）

C# 侧逐条复现，**值必须 bit-for-bit 对齐**：

- [ ] 首次触达 `wm=None` → `[(1990-12-19, end)]`
- [ ] 请求全在未来 → `[]`，end 钳到 today
- [ ] 已覆盖且新鲜（stale=false 且 end≤last_date）→ `[]`
- [ ] 头部缺口 `start < first_date` → `[(start, first_date-1)]`
- [ ] 尾部缺口·触及定型区 → 立即补
- [ ] 尾部缺口·仅未定型 且 fresh → **不补**（节流）
- [ ] 尾部缺口·仅未定型 且 stale → 补
- [ ] 未定型刷新·起点回退到上次抓取定型边界+1（k_d=上次抓取日）
- [ ] 三段相邻 → MergeRanges 合并
- [ ] ClaimableLast：空结果定型区照常推进；未定型尾部只认 actual_last
- [ ] 切片：[fs,fe] 跨度>3650 → 多段、升序无缝、每段独立落库推进水位（断点续传）

## 6. 落地顺序（P3→P4）

1. EF Core 实体 + Migrations 建 `kline` / `data_watermark`（重建库）。
2. 移植 `CheckRange`/`ClaimableLast`/`SettledBoundary`/`MergeRanges` + 单测对照（§5）。
3. Python `POST/GET /fetch` + Redis job + 内部 worker（复用现有 `query_k_data`/限流/退避）。
4. dotnet `EnsureRange(k_d)` 串起来：切片 → POST/poll → 落盘事务 → 直读 PG。
5. E2E：新 code 全史回填产生多段、每段推进水位、读穿透命中后直读；对照旧栈同 code 数据。
