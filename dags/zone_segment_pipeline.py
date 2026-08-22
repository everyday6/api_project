"""Silver1 갱신 이벤트로 LION과 Taxi Zone을 다시 연결하는 Silver2 DAG."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.common.alerts import notify_slack_failure
from src.silver2.zone_segment import (
    build_map_zone_segment_staged,
    publish_map_zone_segment,
    validate_reference_inputs,
    validate_staged_map_zone_segment,
)

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}

with DAG(
    dag_id="zone_segment_pipeline",
    description="LION segment와 TLC Taxi Zone의 Silver2 1:1 매핑",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args=default_args,
    tags=["silver2", "lion", "taxi_zone"],
) as dag:
    validate_inputs = PythonOperator(
        task_id="validate_reference_inputs",
        python_callable=validate_reference_inputs,
    )
    stage_mapping = PythonOperator(
        task_id="build_map_zone_segment_staged",
        python_callable=build_map_zone_segment_staged,
    )
    validate_mapping = PythonOperator(
        task_id="validate_staged_map_zone_segment",
        python_callable=validate_staged_map_zone_segment,
        op_kwargs={
            "stage_result": "{{ ti.xcom_pull(task_ids='build_map_zone_segment_staged') }}",
        },
    )
    publish_mapping = PythonOperator(
        task_id="publish_map_zone_segment",
        python_callable=publish_map_zone_segment,
        op_kwargs={
            "validated_stage": "{{ ti.xcom_pull(task_ids='validate_staged_map_zone_segment') }}",
        },
    )

    validate_inputs >> stage_mapping >> validate_mapping >> publish_mapping
