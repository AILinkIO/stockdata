#!/usr/bin/env bash
# 一键构建并拉起 stockdata 单服务（NiceGUI Web + 同步 worker）。
# 透传 docker compose up 的额外参数，如 ./up.sh mcp 只拉起单个服务。
set -euo pipefail
cd "$(dirname "$0")"

DOCKER=(docker)
docker info >/dev/null 2>&1 || DOCKER=(sudo docker)

"${DOCKER[@]}" compose build
"${DOCKER[@]}" compose up -d "$@"
"${DOCKER[@]}" compose ps
