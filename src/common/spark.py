"""Spark/Hadoop 경로 변환 유틸."""


def to_spark_path(path) -> str:
    """S3Path(또는 문자열)를 Spark/Hadoop이 이해하는 s3a:// 문자열로 바꾼다.

    cloudpathlib은 "s3://"를 쓰지만 Hadoop S3A 커넥터는 "s3a://"만 인식한다.
    APP_ENV=local이면 path가 로컬 pathlib.Path라 "s3://"가 원래 없으므로
    이 replace는 그냥 아무 효과 없이 지나간다 — 호출부가 분기를 몰라도 된다.
    """

    return str(path).replace("s3://", "s3a://", 1)
