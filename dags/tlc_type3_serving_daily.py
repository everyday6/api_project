"""S3 Gold2(Type 3 Zone 일별 집계) → RDS 서빙값 발행 파이프라인.

tlc_ingest_daily가 만든 S3 Gold2를 최근 12주 요일별 평균으로 압축해
Zone-Segment 매핑으로 Segment 단위까지 확산한 뒤 RDS에 upsert한다.

tlc_ingest_daily와 강하게 연결하지 않고 독립적으로 매일 실행한다:
  - RDS 발행 단계에 문제가 생겨도(예: RDS 장애) 다운로드부터 다시 실행할
    필요 없이 이 DAG만 재시도하면 된다.
  - Zone-Segment 매핑만 갱신돼도(zone_segment_pipeline) TLC 데이터
    자체는 안 건드려도 이 DAG만 재실행하면 반영된다.
  - "데이터 생성"(tlc_ingest_daily)과 "서비스 DB 발행"(이 DAG)의 역할이
    명확히 나뉜다.

S3 Gold2가 그날 안 바뀌었고 Zone-Segment 매핑도 안 바뀌었으면
check_type3_publish_needed에서 AirflowSkipException으로 바로
끝난다(EMR 안 띄움, 비용 거의 없음) - 그래서 매일 독립적으로 돌려도
낭비가 크지 않다.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag

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
    dag_id="tlc_type3_serving_daily",
    start_date=datetime(2025, 8, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["TLC", "serving", "rds"],
)
def tlc_type3_serving_daily():

    # -----------------------------------------
    # 1. RDS가 최신 12주 S3 Gold2보다 오래된 경우에만 갱신
    # -----------------------------------------

    publish_plan = check_type3_publish_needed()
    reference_ready = check_type3_reference_ready(publish_plan)
    published_values = publish_type3_rolling_values(publish_plan)
    reference_ready >> published_values

    # -----------------------------------------
    # 2. RDS 최신성 확인 (오늘 발행이 있었든 없었든 매일 재확인)
    # -----------------------------------------

    check_type3_rds_freshness(published_values)


# DAG 생성
tlc_type3_serving_daily()
