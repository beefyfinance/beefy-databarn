FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home mcp

# Copy dependency files first (for better layer caching)
COPY mcp/pyproject.toml mcp/uv.lock /app/mcp/

# Resolve everything at build time so the container never needs network to start
WORKDIR /app/mcp
ENV UV_COMPILE_BYTECODE=1
RUN uv sync --frozen --no-dev

COPY mcp/ /app/mcp/

WORKDIR /app
# No PYTHONPATH=/app: a top-level /app/mcp must never shadow the real `mcp` library.
ENV PYTHONUNBUFFERED=1 \
    MCP_DBT_TARGET_DIR=/app/dbt/target

USER mcp
EXPOSE 8090

# Run the venv binary directly (no `uv run`, which could try to sync at startup).
# --limit-concurrency sheds load with 503 instead of queueing unboundedly.
CMD ["/app/mcp/.venv/bin/uvicorn", "--app-dir", "/app/mcp", "server:app", \
     "--host", "0.0.0.0", "--port", "8090", \
     "--limit-concurrency", "200", "--timeout-keep-alive", "5", \
     "--proxy-headers", "--no-server-header"]
