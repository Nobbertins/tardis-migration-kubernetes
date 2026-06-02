FROM python:3.12-slim
RUN pip install aiohttp psutil numpy --break-system-packages
RUN apt-get update && apt-get install -y stress-ng && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY worker.py .
CMD ["python3", "worker.py"]
