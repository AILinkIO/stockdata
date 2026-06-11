# stockdata server：api / fetcher / beat 共用一个镜像，compose 中以不同 command 启动
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

# tzdata：beat 调度与交易日判定依赖 Asia/Shanghai 时区数据
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Shanghai \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# 先装依赖（利用层缓存：lock 不变则不重装）
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# 再拷源码（.dockerignore 排除 .env/.venv/缓存等）
COPY . .

# 默认命令为 API；fetcher/beat 由 compose 覆盖 command
CMD ["uv", "run", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]
