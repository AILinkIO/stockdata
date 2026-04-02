"""
数据源提供者子包。

包含金融数据源的抽象接口、具体实现及相关工具：
- interface.py  — FinancialDataSource 抽象基类与异常定义
- baostock.py   — 基于 Baostock 的具体实现
- context.py    — Baostock 持久会话管理（自动登录/重连/退出登出）
- cache.py      — diskcache 缓存代理（装饰器模式）
"""
