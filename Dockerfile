# syntax=docker/dockerfile:1

FROM python:3.11-slim

# Prevents Python from writing .pyc files and enables unbuffered stdout
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install system dependencies required for psycopg2-binary runtime (libpq/ssl)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create and use non-root user
RUN useradd -m appuser
WORKDIR /app

# Copy dependency list first for better layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py ./

# Default env var (can be overridden)
ENV DATABASE_URL="postgres://pgadmin:SuperSecret123@psql-jarvis-cae-prd.postgres.database.azure.com:5432/postgres?sslmode=require"

USER appuser

# Healthcheck: ensure Python can import required libs
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD ["python", "-c", "import psycopg2, dotenv"]

# Default command runs the main workflow (App Insights logging, DB test, SB test)
CMD ["python", "main.py"]
