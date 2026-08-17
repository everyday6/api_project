"""
TLC Silver 데이터 변환 및 저장

Bronze 파일들을 taxi_type별로 묶어서(청크 4개: yellow/green/fhv/fhvhv):

1. 청크 하나당 Spark 세션 하나로 그 taxi_type의 Bronze Parquet 전부 읽기
2. Silver 형식으로 변환
3. Silver에 파일별 저장

파일마다 Spark 세션을 새로 여는 대신 taxi_type 단위로 세션을 재사용한다.
"""

from pathlib import Path

from airflow.decorators import task

from src.common.config import SILVER_DIR
from src.common.logger import get_logger
from src.common.spark import get_spark

from src.tlc.transform import transform


logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_silver")


@task
def chunk_bronze_files(
    bronze_files: list[dict],
) -> list[list[dict]]:
    """taxi_type별로 묶는다 (yellow/green/fhv/fhvhv 청크 4개).

    build_silver가 청크 하나당 Spark 세션 하나만 열게 하기 위한 준비 단계.
    같은 taxi_type끼리만 묶는 이유: transform()이 taxi_type 하나를 받는
    구조라, 청크 안에 taxi_type이 섞이면 파일마다 다시 분기해야 해서
    복잡해진다.
    """

    grouped: dict[str, list[dict]] = {}
    for bronze_result in bronze_files:
        grouped.setdefault(bronze_result["taxi_type"], []).append(bronze_result)

    chunks = list(grouped.values())

    logger.info(
        f"Silver 청크 {len(chunks)}개 생성 (파일 {len(bronze_files)}개)"
    )

    return chunks


@task(
    pool="silver_pool",
)
def build_silver(
    bronze_chunk: list[dict],
) -> list[dict]:
    """같은 taxi_type의 Bronze 파일 여러 개를 Spark 세션 하나로 Silver 변환한다."""

    spark = get_spark()

    SILVER_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    results = []

    try:

        for bronze_result in bronze_chunk:

            taxi_type = bronze_result["taxi_type"]
            filename = bronze_result["filename"]
            bronze_path = bronze_result["bronze_path"]

            logger.info(
                f"Silver 변환 시작 : {filename}"
            )

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

            results.append({
                "filename": filename,
                "silver_path": str(silver_path),
            })

    except Exception as error:

        logger.error(
            f"Silver 처리 실패 : {error}"
        )

        raise

    finally:
        spark.stop()

    return results