"""Silver1 갱신 이벤트로 LION과 Taxi Zone을 다시 연결하는 Silver2 DAG.

lion_pipeline(Asset("lion_dim_segment_ready"))이나 taxi_zone_pipeline
(Asset("taxi_zone_silver1_updated")) 둘 중 하나만 갱신돼도 자동으로
재실행된다 — 리스트로 넘기면 AND로 해석되므로(둘 다 갱신돼야 트리거),
`|` 연산자로 OR 조건을 명시한다(toll_silver_gold_pipeline과 동일한 패턴).
두 소스가 같은 날 겹쳐도 스케줄러가 중복 실행 없이 하나로 묶어 처리한다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Asset

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
    schedule=Asset("lion_dim_segment_ready") | Asset("taxi_zone_silver1_updated"),
    start_date=datetime(2026, 1, 1),
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
