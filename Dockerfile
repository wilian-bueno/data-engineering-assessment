FROM apache/airflow:2.9.3-python3.11

USER root

# Java is required to run PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    libpq-dev \
    gcc \
    wget \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# PostgreSQL JDBC driver — used by PySpark to write directly to PostgreSQL
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -O /opt/postgresql-jdbc.jar

# Resolve JAVA_HOME dynamically (works on both amd64 and arm64)
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
