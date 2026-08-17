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


# 청크 실행 순서. 동시에 돌 수 있는 청크 수보다 taxi_type이 많으면 누군가는
# 대기하게 되므로, 제일 오래 걸리는 FHVHV를 맨 앞에 둬서 대기 없이 먼저
# 시작하게 한다.
TAXI_TYPE_PRIORITY = ["fhvhv", "yellow", "green", "fhv"]


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

    chunks = [
        grouped[taxi_type]
        for taxi_type in TAXI_TYPE_PRIORITY
        if taxi_type in grouped
    ]

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

            silver_path = (
                SILVER_DIR /
                Path(filename).stem
            )

            # -----------------------------------------
            # 이미 처리된 파일이면 건너뛰기
            # -----------------------------------------
            #
            # 디렉토리 존재 여부만 보면 executor가 쓰는 도중에 죽어서
            # 남은 불완전한 결과물도 "완료"로 착각할 수 있다. Spark는
            # 쓰기가 성공하면 _SUCCESS 마커 파일을 남기므로 그걸로 확인한다.

            if (silver_path / "_SUCCESS").exists():

                logger.info(
                    f"이미 처리된 파일입니다. 건너뜁니다 : {filename}"
                )

                results.append({
                    "filename": filename,
                    "silver_path": str(silver_path),
                })

                continue

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