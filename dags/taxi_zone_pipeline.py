"""정적 TLC Taxi Zone Bronze/Silver1 수동 적재 DAG."""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from src.common.alerts import notify_slack_failure
from src.taxi_zone.bronze import ingest_taxi_zone_lookup, ingest_taxi_zone_shapefile
from src.taxi_zone.silver1 import build as build_taxi_zone_silver1

default_args = {
    "retries": 2,
    "on_failure_callback": notify_slack_failure,
}

with DAG(
    dag_id="taxi_zone_pipeline",
    description="TLC Taxi Zone 정적 참조 데이터 S3 Bronze/Silver1 적재",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["taxi_zone", "static"],
) as dag:
    lookup = PythonOperator(
        task_id="ingest_taxi_zone_lookup",
        python_callable=ingest_taxi_zone_lookup,
    )
    shapefile = PythonOperator(
        task_id="ingest_taxi_zone_shapefile",
        python_callable=ingest_taxi_zone_shapefile,
    )
    silver1 = PythonOperator(
        task_id="build_taxi_zone_silver1",
        python_callable=build_taxi_zone_silver1,
    )
    trigger_zone_segment = TriggerDagRunOperator(
        task_id="trigger_zone_segment_pipeline",
        trigger_dag_id="zone_segment_pipeline",
        trigger_run_id="zone_segment__taxi_zone__{{ run_id }}",
        skip_when_already_exists=True,
        fail_when_dag_is_paused=True,
    )

    [lookup, shapefile] >> silver1 >> trigger_zone_segment
