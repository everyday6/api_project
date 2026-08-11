"""
TLC Silver 적재 모듈

역할:
1. Bronze 데이터 읽기
2. 택시 종류별 데이터를 공통 Silver 형식으로 변환
3. 변환된 DataFrame들을 하나로 병합
4. Silver 영역에 Parquet 형태로 저장

Airflow Task 간 데이터 전달은
커스텀 객체가 아닌 dict를 사용한다.
"""

from pyspark.sql import DataFrame
from airflow.decorators import task

from src.common.config import SILVER_DIR
from src.common.logger import get_logger
from src.common.spark import get_spark

from src.tlc.transform import (
    transform,
    union_all,
)


# =========================================================
# Logger
# =========================================================

logger = get_logger(__name__)


# =========================================================
# Silver 생성
# =========================================================

@task
def build_silver(
    bronze_results: list[dict],
) -> dict:
    """
    Bronze 데이터를 읽어서 Silver 데이터로 변환한다.

    입력:
        [
            {
                "taxi_type": "...",
                "filename": "...",
                "bronze_path": "...",
                "is_new": True
            },
            ...
        ]

    반환:
        {
            "filename": "tlc_silver.parquet",
            "silver_path": "..."
        }
    """

    # -------------------------------------------------
    # 1. Spark Session 생성
    # -------------------------------------------------

    spark = get_spark()

    # -------------------------------------------------
    # 2. Silver 폴더 생성
    # -------------------------------------------------

    SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # 변환된 DataFrame들을 저장할 리스트
    dfs: list[DataFrame] = []

    # -------------------------------------------------
    # 3. Bronze 파일 하나씩 처리
    # -------------------------------------------------

    for bronze in bronze_results:

        filename = bronze["filename"]
        taxi_type = bronze["taxi_type"]
        bronze_path = bronze["bronze_path"]

        try:

            # -----------------------------------------
            # Bronze Parquet 읽기
            # -----------------------------------------

            df = spark.read.parquet(
                bronze_path
            )

            # -----------------------------------------
            # 택시 종류에 맞게 Silver 형식으로 변환
            # -----------------------------------------

            df = transform(
                df=df,
                taxi_type=taxi_type,
            )

            # 변환된 DataFrame 저장
            dfs.append(df)

            logger.info(
                f"변환 완료 : {filename}"
            )

        except Exception as error:

            logger.error(
                f"파일 처리 실패 : "
                f"{filename} ({error})"
            )

            raise

    # -------------------------------------------------
    # 4. DataFrame 병합
    # -------------------------------------------------

    if not dfs:

        raise ValueError(
            "Silver로 변환할 데이터가 없습니다."
        )

    silver_df = union_all(dfs)

    # -------------------------------------------------
    # 5. Silver 저장 경로
    # -------------------------------------------------

    silver_path = (
        SILVER_DIR /
        "tlc_silver.parquet"
    )

    # -------------------------------------------------
    # 6. Silver Parquet 저장
    # -------------------------------------------------

    silver_df.write.mode(
        "overwrite"
    ).parquet(
        str(silver_path)
    )

    logger.info(
        f"Silver 저장 완료 : {silver_path}"
    )

    # -------------------------------------------------
    # 7. Airflow XCom으로 전달
    # -------------------------------------------------
    #
    # SilverResult 객체가 아니라 dict를 반환한다.
    #

    return {
        "filename": "tlc_silver.parquet",
        "silver_path": str(silver_path),
    }