FROM apache/airflow:3.3.0-python3.11

USER root

# Java 설치 (Spark 4.x는 Java 17 이상 필요)
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        gdal-bin && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    ogr2ogr --version && \
    java -version

USER airflow

# Python 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
