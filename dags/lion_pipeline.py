"""
DAG: lion_pipeline

NYC DCP LION(도로망) Bronze와 Silver1을 담당하는 도메인 파이프라인.
분기마다 새 릴리즈가 나오는 전체 스냅샷 데이터라, 증분 개념 없이 매번
통째로 받는다.

검증된 Silver1 dim_segment가 운영 경로에 반영된 뒤에만 Zone-Segment
매핑 DAG를 실행한다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from src.common.alerts import notify_slack_failure
from src.lion.bronze import ingest_lion
from src.lion.silver1 import (
    build_dim_segment_staged,
    cleanup_dim_segment_staging,
    publish_dim_segment,
    validate_staged_dim_segment,
)

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}

with DAG(
    dag_id="lion_pipeline",
    description="LION(도로망) 분기 Bronze/Silver1",
    schedule="0 5 1 1,4,7,10 *",     # 1/4/7/10월 1일 새벽 5시
    start_date=datetime(2025, 1, 1),
    catchup=False,                    # 과거 분기 버전은 지금 굳이 안 채움 (최신 버전이면 충분)
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args=default_args,
    tags=["lion", "quarterly"],
) as dag:

    task_ingest_lion = PythonOperator(
        task_id="ingest_lion",
        python_callable=ingest_lion,
        op_kwargs={
            # 실행일을 그대로 버전 태그로 사용 (파일명이 아니라 "언제 받았는지" 기준)
            "version_date": "{{ ds }}",
        },
    )

    task_build_dim_segment_staged = PythonOperator(
        task_id="build_dim_segment_staged",
        python_callable=build_dim_segment_staged,
        op_kwargs={
            "bronze_version_path": "{{ ti.xcom_pull(task_ids='ingest_lion') }}",
        },
    )

    task_validate_dim_segment = PythonOperator(
        task_id="validate_staged_dim_segment",
        python_callable=validate_staged_dim_segment,
        op_kwargs={
            "stage_result": "{{ ti.xcom_pull(task_ids='build_dim_segment_staged') }}",
        },
    )

    task_publish_dim_segment = PythonOperator(
        task_id="publish_dim_segment",
        python_callable=publish_dim_segment,
        op_kwargs={
            "validated_stage": "{{ ti.xcom_pull(task_ids='validate_staged_dim_segment') }}",
        },
    )

    task_cleanup_dim_segment_staging = PythonOperator(
        task_id="cleanup_dim_segment_staging",
        python_callable=cleanup_dim_segment_staging,
        op_kwargs={
            "published_result": "{{ ti.xcom_pull(task_ids='publish_dim_segment') }}",
        },
    )

    trigger_zone_segment = TriggerDagRunOperator(
        task_id="trigger_zone_segment_pipeline",
        trigger_dag_id="zone_segment_pipeline",
        trigger_run_id="zone_segment__lion__{{ run_id }}",
        skip_when_already_exists=True,
        fail_when_dag_is_paused=True,
    )

    (
        task_ingest_lion
        >> task_build_dim_segment_staged
        >> task_validate_dim_segment
        >> task_publish_dim_segment
    )
    task_publish_dim_segment >> [task_cleanup_dim_segment_staging, trigger_zone_segment]
