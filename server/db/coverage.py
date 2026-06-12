"""
数据覆盖度/新鲜度判定（设计文档 5.4 节规则表的实现）。

本模块是**纯函数**：输入水位行（或 None）与请求范围，输出 Decision——
要么直接读库（fresh），要么给出需要抓取的区间列表。不触碰数据库与时钟
（now 由调用方注入，便于测试）。

核心概念：
- 定型边界 settled_boundary：该日期（含）之前的数据永久有效，之后的数据
  按刷新间隔比对 last_fetched_at。
- 双水位：last_date 判覆盖，last_fetched_at 判新鲜（见设计文档 5.2.2）。
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from db.models import DataWatermark
from settings import settings

# ── 刷新间隔（秒）：请求范围触及未定型区域时，超过该间隔需重抓 ──

_REALTIME = 300
_DAILY = 86400
_WEEKLY = 604800

REFRESH_INTERVALS: dict[str, int] = {
    "k_d": _REALTIME, "k_w": _REALTIME, "k_m": _REALTIME,
    "k_5": _REALTIME, "k_15": _REALTIME, "k_30": _REALTIME, "k_60": _REALTIME,
    "adjust_factor": _REALTIME,
    "stock_basic": _DAILY,
    "dividend": _DAILY,
    "profit": _DAILY, "operation": _DAILY, "growth": _DAILY,
    "balance": _DAILY, "cash_flow": _DAILY, "dupont": _DAILY,
    "express": _DAILY, "forecast": _DAILY,
    "trade_calendar": _DAILY,
    "stock_list": _DAILY,
    "index_sz50": _DAILY, "index_hs300": _DAILY, "index_zz500": _DAILY,
    "industry": _WEEKLY,
    "deposit_rate": _WEEKLY, "loan_rate": _WEEKLY, "rrr": _WEEKLY,
    "money_supply_month": _WEEKLY, "money_supply_year": _WEEKLY,
}

# 首次触达的回填起点（设计文档 5.2.2：以策略保证覆盖连续，杜绝空洞）
_A_SHARE_EPOCH = date(1990, 12, 19)  # 上交所开市


def backfill_start(data_type: str) -> date:
    if data_type in ("k_5", "k_15", "k_30", "k_60"):
        return datetime.strptime(settings.minute_backfill_start, "%Y-%m-%d").date()
    return _A_SHARE_EPOCH


@dataclass
class Decision:
    """覆盖度判定结果。fetch_ranges 为空即可直接读库。"""

    fetch_ranges: list[tuple[date, date]] = field(default_factory=list)
    reason: str = ""

    @property
    def fresh(self) -> bool:
        return not self.fetch_ranges


def _monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def settled_boundary(data_type: str, today: date) -> date:
    """该日期（含）之前的业务数据视为永久定型。"""
    if data_type == "k_w":
        return _monday(today) - timedelta(days=1)  # 本周一之前（周线按周定型）
    if data_type == "k_m":
        return today.replace(day=1) - timedelta(days=1)  # 本月 1 日之前
    if data_type in (
        "deposit_rate", "loan_rate", "rrr", "money_supply_month", "money_supply_year",
    ):
        return today - timedelta(days=60)  # 宏观 2 个月沉淀期
    return today - timedelta(days=1)  # 默认：昨天及以前定型


def claimable_last(
    data_type: str, requested_end: date, actual_last: date | None, today: date
) -> date:
    """抓取完成后水位可声明的 last_date（写侧规则，fetcher 任务调用）。

    已定型部分按请求范围声明（空结果 = 已核实确无数据，照常推进防止反复抓取）；
    未定型尾部只认实际返回的最大业务日期——数据源尚未发布的日期不声明覆盖，
    留待后续请求重抓。否则"抓取时数据源还没有、之后才发布"的日期一旦滑入
    定型区就成为永久空洞（例：收盘后日线延迟更新期间抓当日，空结果却声明
    覆盖当日，次日该日定型，从此不再重抓）。
    """
    claimed = min(requested_end, settled_boundary(data_type, today))
    if actual_last is not None and actual_last > claimed:
        claimed = actual_last
    return claimed


def quarter_disclosure_deadline(year: int, quarter: int) -> date:
    """季报披露截止日：Q1→4/30、Q2→8/31、Q3→10/31、Q4→次年 4/30。"""
    deadlines = {
        1: date(year, 4, 30),
        2: date(year, 8, 31),
        3: date(year, 10, 31),
        4: date(year + 1, 4, 30),
    }
    return deadlines[quarter]


def _is_stale(wm: DataWatermark, data_type: str, now: datetime) -> bool:
    age = (now - wm.last_fetched_at).total_seconds()
    return age > REFRESH_INTERVALS[data_type]


def check_range(
    wm: DataWatermark | None,
    data_type: str,
    start: date,
    end: date,
    now: datetime,
) -> Decision:
    """范围类数据集（K线/因子/快报预告/日历/宏观/分红折算区间）的判定。

    返回的 fetch_ranges 至多三段：头部缺口、尾部缺口、未定型区域刷新。
    """
    today = now.date()
    if data_type != "trade_calendar":
        # 除日历外业务数据不存在于未来：钳制请求尾，避免未来范围每次都判出尾部缺口
        end = min(end, today)
        if end < start:
            return Decision([], "请求范围全部在未来，无可抓取数据")

    if wm is None:
        # 首次触达：从回填起点抓到请求尾（保证覆盖连续）
        fetch_from = min(backfill_start(data_type), start)
        return Decision([(fetch_from, end)], "首次触达，全量回填")

    ranges: list[tuple[date, date]] = []
    boundary = settled_boundary(data_type, today)
    stale = _is_stale(wm, data_type, now)

    # 头部缺口（罕见：回填起点之前的请求）
    if wm.first_date is not None and start < wm.first_date:
        ranges.append((start, wm.first_date - timedelta(days=1)))

    # 尾部缺口。缺口完全落在未定型区时按刷新间隔节流：写侧规则（claimable_last）
    # 保证定型区之外只有实际拿到数据才声明覆盖，因此这种缺口意味着"已请求过、
    # 数据源尚未发布"（如收盘后日线延迟更新），不节流会让每次读请求都空抓一次。
    # 交易日历的未来数据长期有效、随时可抓，不适用该节流。
    if end > wm.last_date:
        gap_unsettled_only = data_type != "trade_calendar" and wm.last_date >= boundary
        if not gap_unsettled_only or stale:
            ranges.append((wm.last_date + timedelta(days=1), end))

    # 未定型区域刷新：起点取**上次抓取时**的定型边界。上次抓取时尚未定型的数据
    # （盘中写入的当日 bar、迟发布的数据）即使现在已滑入定型区也要重新核实，
    # 否则会固化为陈旧 bar 或永久空洞。
    if stale:
        fetched_day = wm.last_fetched_at.astimezone(now.tzinfo).date()
        refresh_from = max(
            start, settled_boundary(data_type, fetched_day) + timedelta(days=1)
        )
        refresh_to = end if data_type == "trade_calendar" else min(end, today)
        if refresh_from <= refresh_to:
            ranges.append((refresh_from, refresh_to))

    if not ranges:
        return Decision([], "已覆盖且新鲜")
    return Decision(_merge_ranges(ranges), "存在缺口或未定型数据过期")


def check_quarter(
    has_rows: bool,
    last_success: datetime | None,
    year: int,
    quarter: int,
    now: datetime,
) -> Decision:
    """六类季度财报的判定。

    季度抓取是点状的，区间水位会对中间未抓取的季度产生虚假覆盖声明，
    因此不用水位表，而用两个点状事实：
    - has_rows：financial_report 中该报告期是否已有数据；
    - last_success：fetch_task 中该 (code, year, quarter) 最近一次成功抓取时刻
      （兼作"已查过但确实没有"的负结果记忆）。
    """
    m, d = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}[quarter]
    quarter_range = [(date(year, m - 2, 1), date(year, m, d))]
    deadline = quarter_disclosure_deadline(year, quarter)
    settled = now.date() > deadline

    if has_rows:
        if settled:
            return Decision([], "披露截止日已过，永久有效")
        if last_success is not None and (
            (now - last_success).total_seconds() <= REFRESH_INTERVALS["profit"]
        ):
            return Decision([], "披露期内但仍新鲜")
        return Decision(quarter_range, "披露期内数据过期")

    # 无数据：区分"从未抓过"与"抓过确实没有"
    if last_success is None:
        return Decision(quarter_range, "该季度未抓取过")
    if settled and last_success.date() > deadline:
        return Decision([], "披露截止日后确认无数据，永久空结果")
    if (now - last_success).total_seconds() <= REFRESH_INTERVALS["profit"]:
        return Decision([], "近期已查过，尚未披露")
    return Decision(quarter_range, "可能已披露，重新检查")


def check_snapshot(
    wm: DataWatermark | None,
    data_type: str,
    snap_date: date,
    has_rows: bool,
    now: datetime,
) -> Decision:
    """快照类数据集（股票列表/成分股/行业/基本信息）的判定。

    has_rows：事实表中该快照日是否已有数据（由调用方查询，本函数保持纯函数）。
    """
    today = now.date()
    snap_range = [(snap_date, snap_date)]

    if not has_rows:
        return Decision(snap_range, "快照不存在")
    if snap_date < today:
        return Decision([], "历史快照永久有效")
    if wm is None or _is_stale(wm, data_type, now):
        return Decision(snap_range, "今日快照过期")
    return Decision([], "今日快照仍新鲜")


def _merge_ranges(ranges: list[tuple[date, date]]) -> list[tuple[date, date]]:
    """合并相邻/重叠区间，减少任务数。"""
    ranges = sorted(ranges)
    merged = [ranges[0]]
    for s, e in ranges[1:]:
        last_s, last_e = merged[-1]
        if s <= last_e + timedelta(days=1):
            merged[-1] = (last_s, max(last_e, e))
        else:
            merged.append((s, e))
    return merged
