"""TLC type=3 Zone Gold2 기록과 Segment DynamoDB 서빙값 적재 태스크."""

from __future__ import annotations

import re
import shutil
from datetime import date
from pathlib import Path
from uuid import uuid4

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

from src.common.config import (
    DYNAMODB_NAV_TABLE,
    GOLD2_DIR,
    SILVER1_DIR,
    SILVER2_DIR,
    TAXI_TYPES,
    TLC_TYPE3_ROLLING_WEEKS,
)
from src.common.dynamodb import get_table
from src.common.logger import get_logger
from src.common.downloader import get_recent_service_months
from src.common.spark import to_spark_path
from src.tlc.emr import run_tlc_emr_operation
from src.tlc.gold2 import (
    TYPE3_META_SEGMENT_ID,
    TYPE3_META_SK,
    select_latest_date_partitions,
)


logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_type3")

TLC_SILVER1_ROOT = SILVER1_DIR / "tlc"
TYPE3_DAILY_ROOT = GOLD2_DIR / "tlc" / "type3_zone_daily"
MAP_ZONE_SEGMENT_PATH = SILVER2_DIR / "map_zone_segment.parquet"
TYPE3_STAGING_ROOT = TYPE3_DAILY_ROOT / "_staging"
TYPE3_MONTH_MARKER_ROOT = TYPE3_DAILY_ROOT / "_month_success"
RUN_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")


def _complete_silver_paths_for_month(
    month: str,
    silver_root=TLC_SILVER1_ROOT,
) -> tuple[list[str], list[str]]:
    """월별 네 taxi_type의 완료된 Silver1 경로와 누락 타입을 반환한다."""

    completed_paths = []
    missing_types = []
    for taxi_type in TAXI_TYPES:
        path = silver_root / f"{taxi_type}_tripdata_{month}"
        if (path / "_SUCCESS").exists():
            completed_paths.append(to_spark_path(path))
        else:
            missing_types.append(taxi_type)
    return completed_paths, missing_types


def _month_success_marker(month: str, marker_root=TYPE3_MONTH_MARKER_ROOT):
    return marker_root / f"month={month}" / "_SUCCESS"


def _staging_run_path(run_id: str, staging_root=TYPE3_STAGING_ROOT):
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"잘못된 Type 3 staging run_id입니다: {run_id}")
    return staging_root / f"run_id={run_id}"


def _find_pending_type3_months(
    service_months: list[str],
    silver_root=TLC_SILVER1_ROOT,
    marker_root=TYPE3_MONTH_MARKER_ROOT,
) -> list[str]:
    """Silver1 네 종류가 완성됐고 Zone Gold2 완료 마커가 없는 월을 찾는다."""

    pending = []
    for month in service_months:
        _, missing_types = _complete_silver_paths_for_month(month, silver_root)
        if not missing_types and not _month_success_marker(month, marker_root).exists():
            pending.append(month)
    return sorted(pending)


def _type3_metadata_is_current(item: dict, window_start: date, window_end: date) -> bool:
    """DynamoDB 메타데이터가 현재 S3 롤링 윈도우를 가리키는지 확인한다."""

    return (
        item.get("status") == "COMPLETED"
        and item.get("window_start") == window_start.isoformat()
        and item.get("window_end") == window_end.isoformat()
    )


def validate_type3_reference(map_zone_segment_path=MAP_ZONE_SEGMENT_PATH) -> str:
    """운영 Type 3에 필요한 Zone-Segment 매핑의 존재를 확인한다."""

    if not map_zone_segment_path.exists():
        raise FileNotFoundError(
            f"Type 3 필수 입력인 zone-segment 매핑이 없습니다: "
            f"{map_zone_segment_path}"
        )
    logger.info("Type 3 zone-segment 매핑 확인 완료: %s", map_zone_segment_path)
    return str(map_zone_segment_path)


@task(trigger_rule="none_failed")
def find_pending_type3_months(_silver_results=None) -> list[str]:
    """신규 Silver가 없어도 최근 Silver1/Zone Gold2 상태를 다시 맞춘다."""

    service_months = [value.strftime("%Y-%m") for value in get_recent_service_months()]
    pending = _find_pending_type3_months(service_months)
    logger.info("Type 3 처리 대기 월: %s", pending)
    return pending


@task(pool="silver_pool")
def build_type3_staged_records(months: list[str]) -> dict:
    """EMR에서 완료되지 않은 월의 Zone Gold2 결과를 임시 경로에 저장한다."""

    if not months:
        raise AirflowSkipException("처리할 Type 3 월이 없어 갱신을 건너뜁니다")

    run_id = uuid4().hex
    run_path = _staging_run_path(run_id)
    ready_months = []
    for month in months:
        silver_paths, missing_types = _complete_silver_paths_for_month(month)
        if missing_types:
            logger.info(
                "Type 3 월 계산 보류: month=%s missing_taxi_types=%s",
                month,
                missing_types,
            )
            continue
        ready_months.append({"month": month, "silver_paths": silver_paths})

    if not ready_months:
        raise AirflowSkipException(
            "네 taxi_type이 모두 준비된 신규 월이 없어 Type 3 갱신을 건너뜁니다"
        )

    return run_tlc_emr_operation(
        "build_type3_stage",
        {
            "run_id": run_id,
            "run_path": str(run_path),
            "months": ready_months,
        },
    )


@task(pool="silver_pool")
def validate_type3_staged_records(stage_result: dict) -> dict:
    """EMR에서 월별 임시 결과를 검증하고 승격 계획을 반환한다."""

    _staging_run_path(stage_result["run_id"])
    return run_tlc_emr_operation(
        "validate_type3_stage",
        {"stage_result": stage_result},
    )


@task(pool="silver_pool")
def publish_type3_daily_records(validated_stage: dict) -> dict:
    """EMR에서 검증된 날짜 파티션을 운영 경로에 반영한다."""

    _staging_run_path(validated_stage["run_id"])
    return run_tlc_emr_operation(
        "publish_type3_daily",
        {
            "validated_stage": validated_stage,
            "daily_root": str(TYPE3_DAILY_ROOT),
            "marker_root": str(TYPE3_MONTH_MARKER_ROOT),
        },
    )


@task
def cleanup_type3_staging(published_result: dict) -> None:
    """운영 경로 승격에 성공한 실행의 임시 결과를 삭제한다."""

    run_path = _staging_run_path(published_result["run_id"])
    if not run_path.exists():
        return
    if isinstance(run_path, Path):
        shutil.rmtree(run_path)
    else:
        run_path.rmtree()
    logger.info("Type 3 staging 정리 완료: %s", run_path)


@task(trigger_rule="none_failed")
def check_type3_publish_needed(_published_result=None) -> dict:
    """S3 최신 N주와 DynamoDB 메타데이터를 비교해 적재 여부를 판단한다."""

    if not DYNAMODB_NAV_TABLE:
        raise ValueError("DYNAMODB_NAV_TABLE 환경변수가 필요합니다")

    _, window_start, window_end = select_latest_date_partitions(
        TYPE3_DAILY_ROOT.glob("date=*"),
        TLC_TYPE3_ROLLING_WEEKS * 7,
    )
    table = get_table(DYNAMODB_NAV_TABLE)
    response = table.get_item(
        Key={"segment_id": TYPE3_META_SEGMENT_ID, "sk": TYPE3_META_SK},
        ConsistentRead=True,
    )
    metadata = response.get("Item", {})
    if _type3_metadata_is_current(metadata, window_start, window_end):
        raise AirflowSkipException(
            f"DynamoDB Type 3가 이미 최신입니다: {window_start}~{window_end}"
        )

    logger.info(
        "DynamoDB Type 3 갱신 필요: current_window_end=%s target=%s~%s",
        metadata.get("window_end"),
        window_start,
        window_end,
    )
    return {
        "daily_path": str(TYPE3_DAILY_ROOT),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
    }


@task(pool="silver_pool")
def publish_type3_rolling_values(publish_plan: dict) -> dict:
    """EMR에서 최근 N주 평균을 계산하고 DynamoDB에 적재한다."""

    if not DYNAMODB_NAV_TABLE:
        raise ValueError("DYNAMODB_NAV_TABLE 환경변수가 필요합니다")

    daily_path = publish_plan["daily_path"]
    if daily_path != str(TYPE3_DAILY_ROOT):
        raise ValueError(f"예상하지 못한 Type 3 날짜별 경로입니다: {daily_path}")

    return run_tlc_emr_operation(
        "publish_type3_rolling",
        {
            "publish_plan": publish_plan,
            "daily_root": str(TYPE3_DAILY_ROOT),
            "mapping_path": str(MAP_ZONE_SEGMENT_PATH),
            "table_name": DYNAMODB_NAV_TABLE,
            "rolling_weeks": TLC_TYPE3_ROLLING_WEEKS,
        },
    )
