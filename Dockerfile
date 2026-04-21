FROM python:3.12-slim

WORKDIR /app

COPY src/ingestion/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ingestion/ ./src/ingestion/

CMD ["python", "-u", "src/ingestion/at_producer.py"]
