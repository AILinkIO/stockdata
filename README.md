# stockdata

A 股数据相关项目的单一仓库（monorepo）。各项目位于独立子目录，互不耦合，
各自管理自己的依赖（uv）与部署单元。

## 项目索引

| 目录 | 项目 | 说明 |
|---|---|---|
| [server/](server/) | A股数据服务 | FastAPI REST + Celery 抓取任务体系 + PostgreSQL 数据仓库，数据源 Baostock。详见 [server/README.md](server/README.md) |

## 约定

- 每个项目子目录是独立的 uv 工程（自带 `pyproject.toml` / `uv.lock` / `.venv` / `.env`），
  命令在**各自目录内**执行（如 `cd server && uv run pytest`）；
- 新项目平级新增子目录，并在上表登记；
- 根目录只保留仓库级文件（LICENSE、.gitignore、本 README）。
