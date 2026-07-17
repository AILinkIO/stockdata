-- stockdata v2 schema：全部幂等（CREATE TABLE IF NOT EXISTS），由 `stockdata db init` 执行。
-- 价格全部存不复权原始值，复权读时由 adjust_factor 计算。

CREATE TABLE IF NOT EXISTS schema_meta (
    version    int PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

-- 全市场证券表（query_stock_basic 全量）
CREATE TABLE IF NOT EXISTS security (
    code       text PRIMARY KEY,               -- 'sh.600000'
    code_name  text NOT NULL DEFAULT '',
    ipo_date   date,
    out_date   date,
    type       smallint,                       -- 1 股票 2 指数 3 其他
    status     smallint,                       -- 1 上市 0 退市
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Web 关注列表（web 唯一可写业务表）
CREATE TABLE IF NOT EXISTS watchlist (
    code     text PRIMARY KEY,
    note     text NOT NULL DEFAULT '',
    added_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trade_calendar (
    calendar_date  date PRIMARY KEY,
    is_trading_day boolean NOT NULL
);

-- 日/周 K 线合表；日线独有列对周线为 NULL
CREATE TABLE IF NOT EXISTS kline (
    code         text NOT NULL,
    frequency    text NOT NULL CHECK (frequency IN ('d', 'w')),
    trade_date   date NOT NULL,
    open         numeric,
    high         numeric,
    low          numeric,
    close        numeric,
    volume       bigint,
    amount       numeric,
    turn         numeric,
    pct_chg      numeric,
    preclose     numeric,        -- 以下日线独有
    trade_status smallint,
    is_st        boolean,
    pe_ttm       numeric,
    pb_mrq       numeric,
    ps_ttm       numeric,
    pcf_ncf_ttm  numeric,
    PRIMARY KEY (code, frequency, trade_date)
);
CREATE INDEX IF NOT EXISTS kline_freq_date_idx ON kline (frequency, trade_date);

CREATE TABLE IF NOT EXISTS kline_minute (
    code      text NOT NULL,
    frequency text NOT NULL CHECK (frequency IN ('5', '30')),
    bar_time  timestamptz NOT NULL,            -- bar 结束时刻（Asia/Shanghai）
    open      numeric,
    high      numeric,
    low       numeric,
    close     numeric,
    volume    bigint,
    amount    numeric,
    PRIMARY KEY (code, frequency, bar_time)
);

CREATE TABLE IF NOT EXISTS adjust_factor (
    code               text NOT NULL,
    divid_operate_date date NOT NULL,          -- 除权除息日
    fore_adjust_factor numeric,
    back_adjust_factor numeric,
    adjust_factor      numeric,
    PRIMARY KEY (code, divid_operate_date)
);

CREATE TABLE IF NOT EXISTS dividend (
    code               text NOT NULL,
    plan_announce_date date NOT NULL,
    year_type          text NOT NULL,          -- report / operate
    operate_date       date,                   -- 除权除息日：驱动 adjust_factor 事件重抓
    detail             jsonb NOT NULL,
    PRIMARY KEY (code, plan_announce_date, year_type)
);
CREATE INDEX IF NOT EXISTS dividend_operate_idx ON dividend (code, operate_date);

-- 8 类：profit/operation/growth/balance/cash_flow/dupont/performance_express/forecast
CREATE TABLE IF NOT EXISTS financial_report (
    code        text NOT NULL,
    report_type text NOT NULL,
    stat_date   date NOT NULL,                 -- 报告期
    pub_date    date,
    metrics     jsonb NOT NULL,
    PRIMARY KEY (code, report_type, stat_date)
);

CREATE TABLE IF NOT EXISTS stock_industry (
    snap_date               date NOT NULL,
    code                    text NOT NULL,
    industry                text,
    industry_classification text,
    PRIMARY KEY (snap_date, code)
);

CREATE TABLE IF NOT EXISTS index_constituent (
    index_code text NOT NULL CHECK (index_code IN ('sz50', 'hs300', 'zz500')),
    snap_date  date NOT NULL,
    code       text NOT NULL,
    code_name  text,
    PRIMARY KEY (index_code, snap_date, code)
);

CREATE TABLE IF NOT EXISTS stock_list_snapshot (
    snap_date    date NOT NULL,
    code         text NOT NULL,
    code_name    text,
    trade_status smallint,
    PRIMARY KEY (snap_date, code)
);

-- 宏观合并表：deposit_rate / loan_rate / rrr / money_supply_month / money_supply_year
CREATE TABLE IF NOT EXISTS macro_data (
    kind     text NOT NULL,
    date_key text NOT NULL,                    -- pubDate / 'YYYY-MM' / 'YYYY'
    payload  jsonb NOT NULL,
    PRIMARY KEY (kind, date_key)
);

-- 每 (code, dataset) 的同步水位 = 断点；市场级数据集 code=''
CREATE TABLE IF NOT EXISTS sync_watermark (
    code           text NOT NULL,
    dataset        text NOT NULL,
    first_date     date,
    last_date      date,                       -- 覆盖到（含）：业务水位
    last_synced_at timestamptz,                -- 上次成功同步时刻：系统水位
    PRIMARY KEY (code, dataset)
);
CREATE INDEX IF NOT EXISTS sync_watermark_dataset_idx ON sync_watermark (dataset, last_date);

-- 运行历史（Web/CLI 展示）
CREATE TABLE IF NOT EXISTS sync_run (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    params      jsonb NOT NULL DEFAULT '{}'::jsonb,
    status      text NOT NULL DEFAULT 'running',  -- running/done/stopped/halted/failed
    stats       jsonb NOT NULL DEFAULT '{}'::jsonb
);

-- baostock 登录时间戳（单行）：≥5 分钟登录间隔红线的持久化载体
CREATE TABLE IF NOT EXISTS baostock_session (
    id            boolean PRIMARY KEY DEFAULT true CHECK (id),
    last_login_at timestamptz
);

-- 全局同步状态：key='halt' → {"reason": ..., "at": ...}（拉黑熔断持久化）
CREATE TABLE IF NOT EXISTS sync_state (
    key        text PRIMARY KEY,
    value      jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO baostock_session (id, last_login_at) VALUES (true, NULL) ON CONFLICT DO NOTHING;
INSERT INTO schema_meta (version) VALUES (1) ON CONFLICT DO NOTHING;
