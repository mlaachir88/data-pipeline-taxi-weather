FROM apache/airflow:2.9.1

COPY requirements.txt /

USER root
RUN apt-get update && apt-get install -y gcc libpq-dev && apt-get clean

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
