"""stockdata：A 股数据同步与展示。

单常驻进程 = NiceGUI Web + 唯一 baostock 同步 worker 线程；
CLI 是 HTTP 薄客户端（启动任务 + 展示进度）。
"""
