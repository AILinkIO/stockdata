# dotnet-mcp 实施任务清单

> 项目：C# MCP 服务（baostock 能力透传 + TA-Lib 指标分析）
> 架构决策：MCP 不直连 baostock，透传 = 代理 server REST API（:8080）；
> 指标计算的数据同样走 REST（前复权 K 线）。MCP 监听 :8000（旧 MCP 端口复用，
> 存量客户端配置不变）。
> 技术栈：.NET 10 (LTS) / ModelContextProtocol SDK v1.2+ (Streamable HTTP) /
> TALib.NETCore（纯托管移植）

## 阶段 A：工程骨架与 SDK 冒烟（关键路径）

- [x] A1 解决方案骨架：`StockData.Mcp.sln` + `src/StockData.Mcp`（ASP.NET Core）
      + `tests/StockData.Mcp.Tests`（xUnit）
- [x] A2 引入 `ModelContextProtocol.AspNetCore`、`TALib.NETCore`、
      `Microsoft.Extensions.Http.Resilience`，确认 net10.0 下编译通过
- [x] A3 MCP host：Streamable HTTP 监听 :8000，端点 `/mcp`
- [x] A4 REST typed client 骨架：BaseUrl 配置（默认 http://127.0.0.1:8080）、
      超时 ≥90s、504 重试策略、错误 → "Error: ..." 字符串映射
- [x] A5 第一个透传工具 `get_latest_trading_date` 端到端
- [x] A6 冒烟验收：JSON-RPC 走 Streamable HTTP 完成 initialize → tools/list →
      tools/call，拿到真实数据

## 阶段 B：透传工具组（28 个，名称与旧 Python MCP 兼容）

- [x] B1 行情：get_historical_k_data（含分钟线分流）、get_stock_basic_info、
      get_dividend_data、get_adjust_factor_data
- [x] B2 财报：get_profit/operation/growth/balance/cash_flow/dupont_data、
      get_performance_express_report、get_forecast_report、get_fina_indicator
- [x] B3 指数与行业：get_index_constituents、get_sz50/hs300/zz500_stocks、
      get_stock_industry、list_industries、get_industry_members
- [x] B4 市场与宏观：get_trade_dates、get_all_stock、search_stocks、
      get_suspensions、get_deposit_rate_data、get_loan_rate_data、
      get_required_reserve_ratio_data、get_money_supply_data_month/year
- [x] B5 日期与工具：is_trading_day、previous/next_trading_day、
      get_last_n_trading_days、get_recent_trading_range、normalize_stock_code、
      normalize_index_code、get_stock_analysis
- [x] B6 验收：tools/list 数量与签名对照旧 mcp_shim 清单（commit 8eee679）；
      抽查 K 线/财报/宏观各 1 个工具的真实调用

## 阶段 C：TA-Lib 指标分析工具组

- [x] C1 指标基础设施：K 线序列加载器（REST 前复权 + double 数组转换）、
      **lookback 注册表**（按指标与参数计算预热长度，自动向前多取并裁剪）
- [x] C2 `calc_indicators`：MA/EMA/MACD/RSI/KDJ/BOLL/ATR/OBV/CCI，
      参数可调，返回序列或最新值
- [x] C3 `detect_candlestick_patterns`：15 个常用形态精选（全量 61 个反射注册收益低，按需扩充注册表），返回命中日期与方向
- [x] C4 `technical_summary`：均线排列/金叉死叉/超买超卖/布林位置的结构化结论
- [x] C5 `compare_indicators`：多标的同指标横向对比
- [x] C6 正确性验收：MA/MACD/RSI 至少 3 个指标与行情软件/已知基准对照；
      KDJ 的 J 值口径核对（TA-Lib STOCH 需自行推导 J）
- [x] C7 单元测试：lookback 计算、指标参数校验、错误路径

## 阶段 D：容器化与收尾

- [ ] D1 Dockerfile（sdk 多阶段构建 → aspnet:10.0 运行时）
- [ ] D2 加入 server/compose.yaml：`mcp` 服务，host 网络，depends_on api
- [ ] D3 根 README 项目索引登记；dotnet-mcp/README.md（启动/配置/工具清单）
- [ ] D4 全栈验收：`docker compose up -d` 一条命令拉起含 MCP 的全栈，
      MCP 客户端实测透传 + 指标各 1 个工具

## 风险与验证记录

| 风险 | 状态 |
|---|---|
| MCP SDK 在 net10.0 的兼容性 | A2 验证 |
| TALib.NETCore 指标口径（尤其 KDJ） | C6 验证 |
| 透传保持数值原样（不经 double） | B 阶段实现约束：透传组直接转发 JSON 文本 |
| 8000 端口空闲 | ✅ 已确认（旧 MCP 已删除） |
| .NET 10 SDK 环境 | ✅ 10.0.301（mise） |
