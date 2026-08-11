FROM apache/airflow:2.8.4-python3.9

USER root

# Java 설치, gdal-bin(ogr2ogr) 설치 - LION(.gdb) 등 File Geodatabase 평탄화용
RUN apt-get update && \
    apt-get install -y default-jdk gdal-bin && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Python 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
