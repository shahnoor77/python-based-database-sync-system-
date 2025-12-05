FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    default-libmysqlclient-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install --no-cache-dir poetry

# Copy Poetry configuration
COPY pyproject.toml poetry.lock README.md ./

# Install dependencies (no virtualenv)
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/data/offsets /app/data/schemas /app/logs

# Run the application
CMD ["python", "-m", "src.main"]
