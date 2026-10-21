FROM python:3.11-slim

RUN pip install --no-cache-dir \
    dbt-duckdb==1.8.4 \
    requests==2.32.3

WORKDIR /dbt
COPY . /dbt
RUN chmod +x /dbt/run.sh

ENTRYPOINT ["bash", "/dbt/run.sh"]
