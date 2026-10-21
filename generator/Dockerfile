FROM python:3.11-slim

RUN pip install --no-cache-dir \
    confluent-kafka[avro]==2.5.0 \
    fastavro==1.9.7 \
    requests==2.32.3 \
    psycopg2-binary==2.9.9

WORKDIR /app
COPY . /app

ENV PYTHONUNBUFFERED=1
CMD ["python", "txn_generator.py"]
