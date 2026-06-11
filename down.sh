#!/usr/bin/env bash
# 一键停止并移除全栈容器（保留数据卷）。
# 透传 docker compose down 的额外参数，如 ./down.sh -v 连同数据卷一起清理。
set -euo pipefail
cd "$(dirname "$0")"

DOCKER=(docker)
docker info >/dev/null 2>&1 || DOCKER=(sudo docker)

"${DOCKER[@]}" compose down "$@"
