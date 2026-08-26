"""TLC Silver1 변환·저장 Airflow 태스크.

Bronze 품질 검증을 통과한 같은 taxi_type 파일 묶음을 Spark 세션 하나로
처리한다. 원본 파일별 출력 디렉터리를 유지해 재실행과 신규 월 증분 처리가
서로 영향을 주지 않게 한다.
"""

from pathlib import Path

from airflow.decorators import task

from src.common.config import EMR_MAX_EXECUTORS_TLC_INGEST, SILVER1_DIR
from src.common.logger import get_logger
from src.tlc.emr import run_tlc_emr_operation

logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_silver1")

SILVER1_ROOT = SILVER1_DIR / "tlc"


@task(pool="tlc_ingest_pool", pool_slots=17)
def build_silver(bronze_chunk: list[dict]) -> list[dict]:
    """EMR에서 Bronze 파일을 공통 스키마로 변환해 S3 Silver1에 저장한다."""

    if not bronze_chunk:
        return []

    SILVER1_ROOT.mkdir(parents=True, exist_ok=True)
    work_items = [
        {
            **item,
            "silver_path": str(SILVER1_ROOT / Path(item["filename"]).stem),
        }
        for item in bronze_chunk
    ]
    result = run_tlc_emr_operation(
        "build_silver",
        {"bronze_chunk": work_items},
        max_executors=EMR_MAX_EXECUTORS_TLC_INGEST,
    )
    return result["results"]
