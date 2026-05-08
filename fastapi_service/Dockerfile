FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY api_model.py .

ENV PORT=8080

EXPOSE 8080

CMD ["sh", "-c", "uvicorn api_model:app --host 0.0.0.0 --port ${PORT}"]