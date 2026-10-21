# Single image for the whole stack: Python 3.10, Java 17, Spark 3.5.9, Airflow.
# Spark is provided by the pinned pyspark wheel (pyspark==3.5.9 in pyproject.toml),
# which bundles the Spark 3.5.9 runtime; only the JVM comes from apt.
FROM python:3.10-slim-bookworm

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.10
ARG UID=1000

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    AIRFLOW_HOME=/opt/airflow \
    PYTHONUNBUFFERED=1

# Airflow, installed against the official constraints file for reproducibility.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Project package. Metadata is installed from the image; at runtime ./src is
# bind-mounted over /opt/project/src, so code edits apply without a rebuild.
WORKDIR /opt/project
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir -e .

RUN useradd --create-home --uid ${UID} airflow \
    && mkdir -p ${AIRFLOW_HOME} /opt/project/data \
    && chown -R airflow:airflow ${AIRFLOW_HOME} /opt/project
USER airflow
