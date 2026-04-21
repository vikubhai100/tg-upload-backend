FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt .

# uvloop install — asyncio se 2x fast event loop
RUN pip install --no-cache-dir -r requirements.txt uvloop

COPY . .

RUN mkdir -p /app/data

EXPOSE 9500

# uvloop use karo — 2x faster async, 2 workers
CMD ["uvicorn", "backend.main:app", \
     "--host", "0.0.0.0", \
     "--port", "9500", \
     "--workers", "2", \
     "--loop", "uvloop", \
     "--timeout-keep-alive", "75"]
