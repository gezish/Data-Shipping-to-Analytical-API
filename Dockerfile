# fastapi_app/Dockerfile
FROM python:3.11-slim

# system deps for asyncpg (libpq)
RUN apt-get update && apt-get install -y build-essential libpq-dev gcc && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

# Use uvicorn as the default command; bind to 0.0.0.0
CMD ["uvicorn", "fastapi_app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
