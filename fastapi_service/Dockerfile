FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY api_model.py .
COPY model_artifact ./model_artifact

EXPOSE 8080

CMD ["sh", "-c", "uvicorn api_model:app --host 0.0.0.0 --port ${AIP_HTTP_PORT:-8080}"]