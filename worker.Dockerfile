FROM python:3.12-slim
RUN pip install aiohttp --break-system-packages
WORKDIR /app
COPY worker.py .
CMD ["python3", "worker.py"]