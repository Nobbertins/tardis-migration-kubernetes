FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir aiohttp psutil

COPY collector_prom.py .

EXPOSE 9100
CMD ["python", "collector_prom.py"]