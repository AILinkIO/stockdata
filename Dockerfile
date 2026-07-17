# stockdata v2 单镜像：NiceGUI 单服务（Web + 同步 worker）+ CLI 同一镜像
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# tzdata：交易日/K 线时间全按 Asia/Shanghai
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Shanghai \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# 先装依赖（层缓存：lock 不变不重装）；--no-install-project 避免源码未拷时装本包
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# 再拷源码装本包（.dockerignore 排除 .env/.venv/tests 等）
COPY src ./src
COPY README.md ./
RUN uv sync --frozen --no-dev

# 运行时冻结：uv run 不重锁/不联网校验
ENV UV_FROZEN=true

# 单服务：Web 页面 + /api/sync/* + 唯一 baostock worker（惰性登录，重启安全）
CMD ["uv", "run", "--no-dev", "stockdata", "serve"]
