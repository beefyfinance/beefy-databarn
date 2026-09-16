FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# Set working directory
WORKDIR /app

# Install system dependencies (util-linux for flock - prevents concurrent dbt runs)
RUN apt-get update && apt-get install -y \
    curl \
    git \
    procps \
    util-linux \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (for better layer caching)
COPY dbt/pyproject.toml dbt/uv.lock /app/dbt/

# Install Python dependencies using uv from dbt project
WORKDIR /app/dbt
RUN uv sync --frozen

# Copy rest of dbt project files
COPY dbt/ /app/dbt/

# Set working directory to dbt project
WORKDIR /app/dbt

# Install dbt packages
RUN uv run dbt deps

# Create logs directory
RUN mkdir -p /app/dbt/logs

# Copy infra scripts (scheduler + lock wrapper + docs publish + Discord helper)
COPY infra/discord_webhook.py /app/infra/dbt/discord_webhook.py
COPY infra/dbt/alerts.py /app/infra/dbt/alerts.py
COPY infra/dbt/run_tests.py /app/infra/dbt/run_tests.py
COPY infra/dbt/scheduler.py /app/infra/dbt/scheduler.py
COPY infra/dbt/run_dbt_with_lock.sh /app/run_dbt_with_lock.sh
COPY infra/dbt/publish_docs.sh /app/publish_docs.sh
RUN chmod +x /app/infra/dbt/scheduler.py /app/run_dbt_with_lock.sh /app/publish_docs.sh

# Set environment variables
ENV DBT_PROFILES_DIR=/app/dbt
ENV DBT_PROJECT_DIR=/app/dbt
ENV DOCGLOW_NO_TELEMETRY=1

# Run the Python scheduler (runs every 30 minutes)
CMD ["uv", "run", "/app/infra/dbt/scheduler.py"]

