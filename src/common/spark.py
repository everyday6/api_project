"""
Spark Session 생성 모듈

역할
1. Spark Session 생성
"""

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Spark Session 반환"""

    return (
        SparkSession.builder
        .appName("NYC TLC Pipeline")
        .master("spark://spark-master:7077")
        # airflow-worker 컨테이너는 airflow-network/spark-network 양쪽에 붙어 있어서
        # 자동 감지된 driver host가 spark-network에서 접근 불가능한 IP로 잡힐 수 있음.
        # spark-network에서도 통하는 compose 서비스 이름으로 고정하고 전체 인터페이스에 bind.
        .config("spark.driver.host", "airflow-worker")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .getOrCreate()
    )