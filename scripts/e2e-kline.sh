#!/usr/bin/env bash
# 日线管线隔离空库 E2E：在本机 PG 上用一个独立空库（默认 stockdata_e2e）跑 dotnet 全链路
# （coverage→切片→fake fetch→EF 落盘→直读→重判新鲜）。**不碰现网 stockdata、不打 baostock。**
#
#   ./scripts/e2e-kline.sh
#
# 前置：本机 PG 在跑；server/.env 有 STOCKDATA_PG_DSN；PG 角色具备 CREATEDB（测试自建/重建该库）。
set -euo pipefail
cd "$(dirname "$0")/.."

ENV_FILE="server/.env"
[ -f "$ENV_FILE" ] || { echo "缺少 $ENV_FILE"; exit 1; }

# 取现网 DSN，把库名换成隔离库（绝不复用 stockdata）
PROD_DSN="$(grep -E '^STOCKDATA_PG_DSN=' "$ENV_FILE" | head -1 | cut -d= -f2-)"
[ -n "$PROD_DSN" ] || { echo "未在 $ENV_FILE 找到 STOCKDATA_PG_DSN"; exit 1; }

E2E_DB="${E2E_DB:-stockdata_e2e}"
[ "$E2E_DB" = "stockdata" ] && { echo "拒绝：隔离库名不能是现网 stockdata"; exit 1; }

# 替换 URL 末尾的库名段 /<db> 为 /<E2E_DB>（去掉最后一个 / 后的内容再拼）
E2E_DSN="${PROD_DSN%/*}/${E2E_DB}"

echo "现网库 : (从 $ENV_FILE，未改动)"
echo "隔离库 : ${E2E_DB}"
echo "E2E DSN: $(printf '%s' "$E2E_DSN" | sed -E 's#://([^:]+):[^@]*@#://\1:***@#')"

# 确保隔离空库存在（owner=现网角色，使其能在库内建表；测试只清表+Migrate，不建/删库）。
PG_OWNER="$(printf '%s' "$PROD_DSN" | sed -E 's#^[a-z+]+://([^:@/]+).*#\1#')"
if ! sudo -n -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='${E2E_DB}'" 2>/dev/null | grep -q 1; then
    echo "建隔离空库 ${E2E_DB}（owner=${PG_OWNER}）..."
    if ! sudo -n -u postgres psql -c "CREATE DATABASE ${E2E_DB} OWNER ${PG_OWNER};" 2>/dev/null; then
        echo "无法自动建库。请用超级用户手动执行一次："
        echo "    sudo -u postgres psql -c \"CREATE DATABASE ${E2E_DB} OWNER ${PG_OWNER};\""
        exit 1
    fi
fi

echo "运行 E2E（测试清表 + Migrate 重建 schema）..."
echo

export STOCKDATA_E2E_PG_DSN="$E2E_DSN"
dotnet test dotnet-mcp/tests/StockData.Mcp.Tests/StockData.Mcp.Tests.csproj \
    --filter "Category=E2E" "$@"
