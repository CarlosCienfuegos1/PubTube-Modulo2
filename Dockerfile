FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt uvicorn

COPY src/ ./src/

ENV PYTHONPATH=/app/src
CMD ["sh", "-c", "cd src && uvicorn api:app --host 0.0.0.0 --port ${PORT:-8000}"]
