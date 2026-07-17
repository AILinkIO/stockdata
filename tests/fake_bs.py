"""假 baostock 模块：单测中 monkeypatch 替换 stockdata.provider.baostock.bs。"""

from __future__ import annotations


class FakeResult:
    """模拟 baostock ResultData：error_code/error_msg/fields + 迭代行。"""

    def __init__(self, error_code="0", error_msg="success", fields=None, rows=None):
        self.error_code = error_code
        self.error_msg = error_msg
        self.fields = fields or []
        self._rows = list(rows or [])
        self._i = 0

    def next(self):
        return self._i < len(self._rows)

    def get_row_data(self):
        row = self._rows[self._i]
        self._i += 1
        return row


class FakeBs:
    """可编程假 baostock：每个查询函数按脚本队列出结果（FakeResult 或 Exception）。"""

    def __init__(self):
        self.login_count = 0
        self.logout_count = 0
        self.login_result = FakeResult()
        self.calls: list[tuple[str, dict]] = []
        self._scripts: dict[str, list] = {}

    def script(self, func_name: str, *results):
        """预置 func_name 的依次返回值（FakeResult 或要抛出的 Exception）。"""
        self._scripts.setdefault(func_name, []).extend(results)

    def login(self):
        self.login_count += 1
        return self.login_result

    def logout(self):
        self.logout_count += 1
        return FakeResult()

    def _dispatch(self, name: str, kwargs: dict):
        self.calls.append((name, kwargs))
        queue = self._scripts.get(name)
        if not queue:
            return FakeResult(fields=["x"], rows=[["1"]])
        result = queue.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def __getattr__(self, name: str):
        if name.startswith("query_"):
            return lambda **kwargs: self._dispatch(name, kwargs)
        raise AttributeError(name)
