ARG AIRFLOW_IMAGE_NAME=apache/airflow:2.10.5
FROM ${AIRFLOW_IMAGE_NAME}

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --default-timeout=1200 --retries=10 --no-cache-dir -r /requirements-airflow.txt
