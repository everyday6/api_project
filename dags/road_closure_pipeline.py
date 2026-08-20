"""
DAG: road_closure_pipeline

도로 통제(road_closures) ingestion. 매주 월요일 새벽에 실행되며, Socrata 원본
전체를 받아 수집일 기준 parquet 스냅샷 하나로 저장한다. 논리적 실행일({{ ds }})은
파일 버전을 구분하는 데만 사용하고 API 행 필터에는 사용하지 않는다.

실제 ingestion/검증 로직은 src/road_closures/bronze.py에 있고, 이 파일은
언제/어떤 순서로 그 함수들을 실행할지만 정의한다.

construction_pipeline(daily)이 여기서 만든 Bronze 파일을 그냥 최신 것으로
읽는다 — 이 파이프라인은 매주 1회만 갱신되므로 Asset으로 daily 파이프라인을
기다리게 하지 않고, 반대로 daily 쪽에서 그때그때 최신 파일을 읽는 방식을
그대로 유지한다(이유는 construction_pipeline.py 참고).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.common.alerts import notify_slack_failure
from src.road_closures.bronze import ingest_road_closures, validate_road_closures

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}

with DAG(
    dag_id="road_closure_pipeline",
    description="도로 통제(road_closures) 원본 전체 스냅샷 갱신",
    schedule="0 4 * * 1",          # 매주 월요일 새벽 4시
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["road_closure", "weekly"],
) as dag:

    task_ingest_road_closures = PythonOperator(
        task_id="ingest_road_closures",
        python_callable=ingest_road_closures,
        op_kwargs={
            # 실행일은 조회 조건이 아니라 스냅샷 파일 버전에만 사용한다.
            "snapshot_date": "{{ ds }}",
        },
    )

    task_validate_road_closures = PythonOperator(
        task_id="validate_road_closures",
        python_callable=validate_road_closures,
        op_kwargs={
            "path": "{{ ti.xcom_pull(task_ids='ingest_road_closures') }}",
        },
    )

    task_ingest_road_closures >> task_validate_road_closures
