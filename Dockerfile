FROM python:3.10-slim

# set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

# install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# copy requirements first for better caching
COPY requirements.txt .

# install python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# copy application code
COPY . .

# create non-root user for security
RUN groupadd -r appuser && useradd --no-log-init -r -g appuser appuser
RUN chown -R appuser:appuser /app
USER appuser

# expose port
EXPOSE 8000

# health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/api/v1/health', timeout=10)" || exit 1

# run the application
CMD ["python", "run_api.py"]
