"""
Silver 적재 모듈

역할
1. Bronze 데이터 읽기
2. 데이터 변환
3. 데이터 병합
4. Silver 저장
"""

from pyspark.sql import DataFrame

from airflow.decorators import task

from src.common.config import (
    SILVER_DIR,
)

from src.common.logger import get_logger

from src.common.spark import get_spark

from src.common.models import (
    BronzeResult,
    SilverResult,
)

from src.tlc.transform import (
    transform,
    union_all,
)

# Logger 생성
logger = get_logger(__name__)


@task
def build_silver(
    bronze_results: list[BronzeResult],
) -> SilverResult:
    """Silver 생성"""

    # Spark Session 생성
    spark = get_spark()

    # Silver 폴더 생성
    SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    dfs: list[DataFrame] = []

    for bronze in bronze_results:

        try:

            # Bronze 읽기
            df = spark.read.parquet(
                str(bronze.bronze_path)
            )

            # 데이터 변환
            df = transform(
                df=df,
                taxi_type=bronze.taxi_type,
            )

            dfs.append(df)

            logger.info(
                f"변환 완료 : {bronze.filename}"
            )

        except Exception as error:

            logger.error(
                f"파일 처리 실패 : {bronze.filename}"
            )

            raise error

    # DataFrame 병합
    silver_df = union_all(dfs)

    # 저장 경로
    silver_path = (
        SILVER_DIR /
        "tlc_silver.parquet"
    )

    # Silver 저장
    silver_df.write.mode(
        "overwrite"
    ).parquet(
        str(silver_path)
    )

    logger.info(
        "Silver 저장 완료"
    )

    return SilverResult(
        filename="tlc_silver.parquet",
        silver_path=silver_path,
    )