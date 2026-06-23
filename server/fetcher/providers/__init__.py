"""数据源 provider。

provider 模块暴露统一的 query_* 函数（见 interface.py 的异常约定），任务层
（fetcher/tasks.py）只依赖这层接口。当前实现：akshare（providers/akshare.py）。
"""
