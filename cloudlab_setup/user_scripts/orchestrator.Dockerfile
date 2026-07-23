FROM python:3.12-slim
RUN pip install aiohttp aiodns --break-system-packages
WORKDIR /app
COPY orchestrator.py .
COPY azurefunctions_2019_day1.txt .
COPY active_funcs.txt .
CMD ["python3", "orchestrator.py"]
