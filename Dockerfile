FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["bash", "-c", "python telegram_scraper.py --channels-file channels.txt --incremental"]
