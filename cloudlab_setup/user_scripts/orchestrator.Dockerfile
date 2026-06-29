FROM python:3.12-slim
RUN pip install aiohttp aiodns --break-system-packages
WORKDIR /app
COPY orchestrator.py .
COPY ../../AzureFunctionsInvocationTraceForTwoWeeksJan2021.txt .
CMD ["python3", "orchestrator.py"]
