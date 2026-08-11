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
        .getOrCreate()
    )