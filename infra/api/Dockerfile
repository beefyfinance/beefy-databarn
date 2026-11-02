FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (for better layer caching)
COPY api/pyproject.toml api/uv.lock /app/api/

# Install Python dependencies using uv
WORKDIR /app/api
RUN uv sync --frozen

# Copy API code
COPY api/ /app/api/

# Set working directory to project root (for imports to work)
WORKDIR /app

# Expose port
EXPOSE 8080

# Run with uvicorn (using api.main:app since we're in /app)
# PYTHONPATH=/app ensures api module can be found
ENV PYTHONPATH=/app
CMD ["uv", "run", "--directory", "api", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8080"]

