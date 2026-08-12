"""
TLC Silver 데이터 변환 및 저장

Bronze 파일 하나를 받아서:

1. Bronze Parquet 읽기
2. Silver 형식으로 변환
3. Silver에 파일별 저장
"""

from pathlib import Path

from airflow.decorators import task

from src.common.config import SILVER_DIR
from src.common.logger import get_logger
from src.common.spark import get_spark

from src.tlc.transform import transform


logger = get_logger(__name__)


@task(
    pool="silver_pool",
)
def build_silver(
    bronze_result: dict,
) -> dict:
    """Bronze 파일 하나를 Silver로 변환한다."""

    spark = get_spark()

    SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    taxi_type = bronze_result["taxi_type"]
    filename = bronze_result["filename"]
    bronze_path = bronze_result["bronze_path"]

    logger.info(
        f"Silver 변환 시작 : {filename}"
    )

    try:

        # -----------------------------------------
        # Bronze 읽기
        # -----------------------------------------

        df = spark.read.parquet(
            str(bronze_path)
        )

        # -----------------------------------------
        # Silver 형식으로 변환
        # -----------------------------------------

        silver_df = transform(
            df=df,
            taxi_type=taxi_type,
        )

        # -----------------------------------------
        # 파일별 Silver 저장
        # -----------------------------------------

        silver_path = (
            SILVER_DIR /
            Path(filename).stem
        )

        silver_df.write.mode(
            "overwrite"
        ).parquet(
            str(silver_path)
        )

        logger.info(
            f"Silver 저장 완료 : {filename}"
        )

        return {
            "filename": filename,
            "silver_path": str(silver_path),
        }

    except Exception as error:

        logger.error(
            f"Silver 처리 실패 : "
            f"{filename} ({error})"
        )

        raise

    finally:
        spark.stop()