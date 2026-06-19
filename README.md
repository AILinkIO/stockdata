# stockdata

A 股数据相关项目的单一仓库（monorepo）。各项目位于独立子目录，互不耦合，
各自管理自己的依赖（uv）与部署单元。

## 项目索引

| 目录 | 项目 | 说明 |
|---|---|---|
| [server/](server/) | A股数据服务 | FastAPI REST + Celery 嵌入式 worker（进程内 solo pool）+ PostgreSQL 数据仓库，数据源 Baostock。详见 [server/README.md](server/README.md) |
| [dotnet-mcp/](dotnet-mcp/) | MCP 服务 | C# (.NET 10) MCP 服务：server REST 透传（38 工具）+ TA-Lib 指标分析（4 工具），Streamable HTTP :8000。详见 [dotnet-mcp/README.md](dotnet-mcp/README.md) |

## 约定

- 每个项目子目录是独立工程，自带各自语言的依赖管理（Python 用 uv：`pyproject.toml` / `uv.lock`；
  .NET 用 SDK：`*.slnx` / `*.csproj`），命令在**各自目录内**执行
  （如 `cd server && uv run pytest`、`cd dotnet-mcp && dotnet test StockData.Mcp.slnx`）；
- 新项目平级新增子目录，并在上表登记；
- 根目录只保留仓库级文件（LICENSE、.gitignore、本 README）。
