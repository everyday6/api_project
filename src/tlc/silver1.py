"""TLC Silver1 변환·저장 Airflow 태스크.

Bronze 품질 검증을 통과한 같은 taxi_type 파일 묶음을 Spark 세션 하나로
처리한다. 원본 파일별 출력 디렉터리를 유지해 재실행과 신규 월 증분 처리가
서로 영향을 주지 않게 한다.
"""

from pathlib import Path

from airflow.decorators import task

from src.common.config import SILVER1_DIR, TAXI_TYPES
from src.common.downloader import build_filename, get_recent_service_months
from src.common.logger import get_logger
from src.tlc.bronze import BRONZE_ROOT
from src.tlc.emr import run_tlc_emr_operation

logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_silver1")

SILVER1_ROOT = SILVER1_DIR / "tlc"


def _find_pending_silver_files(
    service_months,
    bronze_root=BRONZE_ROOT,
    silver_root=SILVER1_ROOT,
) -> list[dict]:
    """Bronze는 있지만 완료된 Silver1이 없는 최근 파일을 찾는다."""

    pending = []
    for service_month in service_months:
        for taxi_type in TAXI_TYPES:
            filename = build_filename(
                taxi_type,
                service_month.year,
                service_month.month,
            )
            bronze_path = bronze_root / filename
            silver_path = silver_root / Path(filename).stem
            if bronze_path.exists() and not (silver_path / "_SUCCESS").exists():
                pending.append({
                    "taxi_type": taxi_type,
                    "filename": filename,
                    "bronze_path": str(bronze_path),
                })
    return pending


@task(trigger_rule="none_failed")
def find_pending_silver_files(_stored_bronze_files=None) -> list[dict]:
    """신규 다운로드가 없어도 최근 Bronze/Silver1 상태를 다시 맞춘다."""

    pending = _find_pending_silver_files(get_recent_service_months())
    logger.info("Silver1 처리 대기 파일: %s개", len(pending))
    return pending


@task(pool="silver_pool")
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
    )
    return result["results"]
