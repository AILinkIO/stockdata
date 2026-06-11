"""
数据源异常层次（自 src/providers/interface.py 迁入）。

原 FinancialDataSource ABC 不再保留：新架构下"可替换数据源"的边界在任务层
（fetcher/tasks.py 的任务签名），provider 模块只需暴露同名查询函数。

- DataSourceError    — 数据源基础异常
  - LoginError       — 数据源登录失败
  - NoDataFoundError — 查询条件下无数据返回（多数任务视为合法的 0 行结果）
"""


class DataSourceError(Exception):
    """数据源基础异常类。"""


class LoginError(DataSourceError):
    """数据源登录失败时抛出。"""


class NoDataFoundError(DataSourceError):
    """查询条件下无数据返回时抛出。"""
