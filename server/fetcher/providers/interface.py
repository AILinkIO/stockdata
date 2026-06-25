"""
数据源异常层次（自 src/providers/interface.py 迁入）。

原 FinancialDataSource ABC 不再保留：新架构下"可替换数据源"的边界在任务层
（fetcher/tasks.py 的任务签名），provider 模块只需暴露同名查询函数。

- DataSourceError    — 数据源基础异常
  - LoginError       — 数据源登录失败
  - NoDataFoundError — 查询条件下无数据返回（多数任务视为合法的 0 行结果）
  - BlacklistError   — 出口 IP 被 baostock 拉黑/接收错误，短期无法取数（致命，停止重试与 worker）
"""


class DataSourceError(Exception):
    """数据源基础异常类。"""


class LoginError(DataSourceError):
    """数据源登录失败时抛出。"""


class NoDataFoundError(DataSourceError):
    """查询条件下无数据返回时抛出。"""


class BlacklistError(DataSourceError):
    """出口 IP 被 baostock 拉黑（10001011）或持续接收错误（10002007）时抛出。

    这类状态会持续很久、短期内无法取数，且每次重试/重登录都再撞 baostock、
    可能延长封禁。故视为致命：不退避重试、不重登录，当前 job 标记 failed 后
    写持久暂停标志（fetch:halted）停止 worker 消费——进程不退、HTTP 保活，
    经 GET /status 暴露、待 POST /restart 恢复。
    """
