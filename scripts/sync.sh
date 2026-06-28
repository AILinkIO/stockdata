#!/usr/bin/env bash
# sync-cli 包装：cron 友好入口。
#
# 用法：
#   ./scripts/sync.sh drain              # 一次性 drain（消费 stock_sync_task 直到空/halt）
#   ./scripts/sync.sh drain --code sh.600000
#   ./scripts/sync.sh market             # 单跑市场级数据
#   ./scripts/sync.sh retry              # 把 failed 任务重置为 pending（drain 下次会捡起）
#   ./scripts/sync.sh status             # 队列进度观测（只读）
#
# Cron 推荐（防 overlap 用 flock）：
#   0 */4 * * *  flock -n /tmp/sync.lock /path/to/scripts/sync.sh drain       >> /var/log/sync.log 2>&1
#   30 6 * * *   flock -n /tmp/sync-market.lock /path/to/scripts/sync.sh market >> /var/log/sync-market.log 2>&1
#
# 手动看 TUI（真 TTY 自动开 Terminal.Gui dashboard）：
#   ./scripts/sync.sh drain
set -euo pipefail

# 切到仓库根（脚本相对路径解析）
cd "$(dirname "$0")/.."

# 加载 PG/fetch 共享 .env（与 compose env_file 同源）
if [ -f server/.env ]; then
    set -a
    . server/.env
    set +a
fi

# --no-build: 依赖宿主机预先 dotnet build；cron 重复调用零开销
# 若改了源码，先 dotnet build dotnet-mcp/src/StockData.SyncCli
exec dotnet run --project dotnet-mcp/src/StockData.SyncCli --no-build -- "$@"