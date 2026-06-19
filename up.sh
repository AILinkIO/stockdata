#!/usr/bin/env bash
# 一键构建并拉起全栈：migrate → api（内嵌 worker + beat）+ mcp（dotnet-mcp/）。
# 透传 docker compose up 的额外参数，如 ./up.sh mcp 只拉起单个服务。
set -euo pipefail
cd "$(dirname "$0")"

DOCKER=(docker)
docker info >/dev/null 2>&1 || DOCKER=(sudo docker)

"${DOCKER[@]}" compose build
"${DOCKER[@]}" compose up -d "$@"
"${DOCKER[@]}" compose ps
