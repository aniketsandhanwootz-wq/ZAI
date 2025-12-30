FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (optional but safe)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install python deps
COPY service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy code + packages + contracts
COPY service/app /app/app
COPY packages /app/packages

# Keep legacy absolute path working (/packages/...)
RUN ln -s /app/packages /packages

EXPOSE 8000

# Run API + RQ worker in same container (two processes)
CMD ["sh", "-c", "\
  QUEUES=$(echo \"${CONSUMER_QUEUES:-default}\" | tr ',' ' '); \
  echo \"Starting RQ worker on queues: $QUEUES\"; \
  rq worker $QUEUES --url \"$REDIS_URL\" & \
  exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} \
"]

