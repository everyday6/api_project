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
        # spark-worker가 1대(10코어)뿐이고 silver_pool로 최대 3개 태스크가
        # 동시에 이 함수를 호출하므로, 앱 하나가 등록 순서에 따라 코어를
        # 몰아 받지 않도록 앱당 코어를 3개(10 ÷ 3)로 고정한다.
        .config("spark.cores.max", "3")
        # 기본값 200은 대규모 클러스터 기준이라, 코어 3개짜리 앱이 월별
        # 파일 하나를 처리하기엔 파티션이 지나치게 많아 스케줄링 오버헤드만 커진다.
        # 앱이 실제로 쓰는 코어 수에 맞춘다.
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )