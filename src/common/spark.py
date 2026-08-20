"""
Spark Session 생성 모듈

역할
1. Spark Session 생성
"""

from pyspark.sql import SparkSession

from src.common.config import AWS_REGION


def to_spark_path(path) -> str:
    """S3Path(또는 문자열)를 Spark/Hadoop이 이해하는 s3a:// 문자열로 바꾼다.

    cloudpathlib은 "s3://"를 쓰지만 Hadoop S3A 커넥터는 "s3a://"만 인식한다.
    """

    return str(path).replace("s3://", "s3a://", 1)


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
        # Bronze/Silver가 S3(s3a://)에 있어서 Spark에 Hadoop S3 커넥터가
        # 필요하다. spark-worker 이미지의 Hadoop이 3.4.1이라 정확히 맞춘
        # hadoop-aws를 쓴다(버전이 안 맞으면 클래스 충돌로 조용히 깨짐).
        # 자격증명은 EC2 인스턴스 롤(DefaultAWSCredentialsProviderChain이
        # 자동으로 찾음)을 쓰므로 여기 access key를 박지 않는다.
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
        .getOrCreate()
    )