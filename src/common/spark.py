"""
Spark Session 생성 모듈

역할
1. Spark Session 생성
"""

from pyspark.sql import SparkSession

from src.common.config import APP_ENV, AWS_REGION


def to_spark_path(path) -> str:
    """S3Path(또는 문자열)를 Spark/Hadoop이 이해하는 s3a:// 문자열로 바꾼다.

    cloudpathlib은 "s3://"를 쓰지만 Hadoop S3A 커넥터는 "s3a://"만 인식한다.
    APP_ENV=local이면 path가 로컬 pathlib.Path라 "s3://"가 원래 없으므로
    이 replace는 그냥 아무 효과 없이 지나간다 — 호출부가 분기를 몰라도 된다.
    """

    return str(path).replace("s3://", "s3a://", 1)


def get_spark() -> SparkSession:
    """Spark Session 반환.

    APP_ENV=local이면 spark-master 클러스터에 붙지 않고 이 프로세스 안에서
    단일 JVM으로 돈다(local[*]) — S3A 커넥터(hadoop-aws)도 필요 없다(로컬
    디스크를 그냥 읽으므로). 나머지 튜닝(코어 수 고정, shuffle partitions)은
    클러스터 자원 경합을 피하기 위한 설정이라 로컬 단일 프로세스에는
    의미가 없어서 뺀다.
    """

    if APP_ENV == "local":
        return (
            SparkSession.builder
            .appName("NYC TLC Pipeline (local)")
            .master("local[*]")
            .getOrCreate()
        )

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