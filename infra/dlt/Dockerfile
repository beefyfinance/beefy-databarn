FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    procps \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (for better layer caching)
COPY dlt/pyproject.toml dlt/uv.lock /app/dlt/

# Install Python dependencies using uv
WORKDIR /app/dlt
RUN uv sync --frozen

# Copy dlt project files
COPY dlt/ /app/dlt/

COPY infra/dlt/scheduler.py infra/dlt/entrypoint.sh infra/dlt/set_dlt_env.sh /app/infra/dlt/
RUN chmod +x /app/infra/dlt/scheduler.py /app/infra/dlt/entrypoint.sh /app/infra/dlt/set_dlt_env.sh

# Set working directory to dlt project
WORKDIR /app/dlt

ENTRYPOINT ["/app/infra/dlt/entrypoint.sh"]
# Run the Python scheduler (runs every 5 minutes)
CMD ["uv", "run", "/app/infra/dlt/scheduler.py"]

