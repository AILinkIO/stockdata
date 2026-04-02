"""
数据源单例模块。

提供全局唯一的金融数据源实例，供所有工具模块直接 import 使用。
当前实现为 Baostock，外层包裹 diskcache 缓存代理。
如需切换数据源，只需修改此处的实例化类。
"""
from src.providers.baostock import BaostockDataSource
from src.providers.cache import CachedDataSource
from src.providers.interface import FinancialDataSource

active_data_source: FinancialDataSource = CachedDataSource(BaostockDataSource())
