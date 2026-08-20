"""
TLC Silver1 — Bronze 파일 청크 분류 + 변환 + 저장 (Airflow 태스크)

Bronze 파일들을 taxi_type별로 묶어서(청크 4개: yellow/green/fhv/fhvhv):
1. 청크 하나당 Spark 세션 하나로 그 taxi_type의 Bronze Parquet 전부 읽기
2. src/tlc/silver1_transform.py의 transform()으로 Silver1 형식 변환
3. Silver1에 파일별 저장

파일마다 Spark 세션을 새로 여는 대신 taxi_type 단위로 세션을 재사용한다.

실제 변환 로직(컬럼명 통일, 타입 캐스팅 등)은 airflow 의존이 없는
src/tlc/silver1_transform.py에 있다 — src/tlc/expectations.py처럼 순수
변환 로직만 필요한 소비처가 airflow 설치 없이도 import할 수 있게 하기
위함이다.
"""

from pathlib import Path

from airflow.decorators import task
from airflow.sdk import Asset

from src.common.config import SILVER1_DIR
from src.common.logger import get_logger
from src.common.spark import get_spark
from src.tlc.silver1_transform import transform


logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_silver")

# tlc_pipeline / tlc_daily 둘 다 이 태스크를 통해 Silver1을 만들므로,
# outlet을 여기 하나에만 걸면 두 DAG 모두에서 자동으로 발행된다.
# tlc_gold_volume이 이 Asset을 구독해서 새 Silver1이 생길 때만 재계산한다.
TLC_SILVER = Asset("tlc_silver")

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
        f"Silver1 청크 {len(chunks)}개 생성 (파일 {len(bronze_files)}개)"
    )

    return chunks


@task(
    pool="silver_pool",
    outlets=[TLC_SILVER],
)
def build_silver(
    bronze_chunk: list[dict],
) -> list[dict]:
    """같은 taxi_type의 Bronze 파일 여러 개를 Spark 세션 하나로 Silver1 변환한다."""

    if not bronze_chunk:
        return []

    spark = get_spark()

    SILVER1_DIR.mkdir(
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
                SILVER1_DIR /
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
                f"Silver1 변환 시작 : {filename}"
            )

            # -----------------------------------------
            # Bronze 읽기
            # -----------------------------------------

            df = spark.read.parquet(
                str(bronze_path)
            )

            # -----------------------------------------
            # Silver1 형식으로 변환
            # -----------------------------------------

            silver_df = transform(
                df=df,
                taxi_type=taxi_type,
            )

            # -----------------------------------------
            # 파일별 Silver1 저장
            # -----------------------------------------

            silver_df.write.mode(
                "overwrite"
            ).parquet(
                str(silver_path)
            )

            logger.info(
                f"Silver1 저장 완료 : {filename}"
            )

            results.append({
                "filename": filename,
                "silver_path": str(silver_path),
            })

    except Exception as error:

        logger.error(
            f"Silver1 처리 실패 : {error}"
        )

        raise

    finally:
        spark.stop()

    return results
