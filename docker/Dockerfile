FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml .
# COPY README.md . # Optional if referenced
COPY core/ core/
COPY web/ web/
COPY scripts/ scripts/
COPY data/ data/

# Install python dependencies
# Note: Creating a temporary requirements.txt or installing directly if pyproject is compliant
RUN pip install --no-cache-dir .

# Default command (overridden by docker-compose)
CMD ["python3", "core/ingestion/polling_engine.py"]
