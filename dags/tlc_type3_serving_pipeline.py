"""S3 Gold2(Type 3 Zone 일별 집계) → RDS 서빙값 발행 파이프라인.

tlc_ingest_pipeline이 만든 S3 Gold2를 최근 12주 요일별 평균으로 압축해
Zone-Segment 매핑으로 Segment 단위까지 확산한 뒤 RDS에 upsert한다.

cron polling 없이 두 Asset 중 하나가 발행될 때 실행한다:
  - tlc_type3_gold2_ready: tlc_ingest_pipeline이 새 Gold2 월을 발행함
  - map_zone_segment_ready: LION/Taxi Zone 변경으로 매핑이 갱신됨

RDS 발행 단계에 문제가 생겨도(예: RDS 장애) 다운로드부터 다시 실행할
필요 없이 이 DAG만 재시도하면 되고, "데이터 생성"과 "서비스 DB 발행"의
역할도 분리된다.

두 Asset이 짧은 간격으로 연속 발행되어 중복 run이 생기더라도
check_type3_publish_needed에서 AirflowSkipException으로 바로
끝난다(EMR 안 띄움, 비용 거의 없음).
"""

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.sdk import Asset

from src.common.alerts import notify_slack_failure
from src.tlc.type3_pipeline import (
    check_type3_publish_needed,
    check_type3_reference_ready,
    check_type3_rds_freshness,
    publish_type3_rolling_values,
)

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}


# =========================================================
# DAG
# =========================================================

@dag(
    dag_id="tlc_type3_serving_pipeline",
    start_date=datetime(2025, 8, 1),
    schedule=(
        Asset("tlc_type3_gold2_ready")
        | Asset("map_zone_segment_ready")
    ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["TLC", "serving", "rds"],
)
def tlc_type3_serving_pipeline():

    # -----------------------------------------
    # 1. RDS가 최신 12주 S3 Gold2보다 오래된 경우에만 갱신
    # -----------------------------------------

    publish_plan = check_type3_publish_needed()
    reference_ready = check_type3_reference_ready(publish_plan)
    published_values = publish_type3_rolling_values(publish_plan)
    reference_ready >> published_values

    # -----------------------------------------
    # 2. RDS 최신성 확인 (Asset 이벤트 처리 시마다 확인)
    # -----------------------------------------

    check_type3_rds_freshness(published_values)


# DAG 생성
tlc_type3_serving_pipeline()
