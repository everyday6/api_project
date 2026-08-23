"""정적 TLC Taxi Zone Bronze/Silver1 수동 적재 DAG.

build_taxi_zone_silver1은 Asset("taxi_zone_silver1_updated")를 outlet으로
내보낸다 — zone_segment_pipeline이 lion_pipeline의 lion_dim_segment_ready와
함께 이 Asset을 구독해서, 둘 중 하나만 갱신돼도 자동으로 재실행된다
(TriggerDagRunOperator로 DAG 이름을 직접 지정하던 방식에서 전환 — 두 소스가
같은 날 겹치면 중복 실행되던 문제가 있었음).
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Asset

from src.common.alerts import notify_slack_failure
from src.taxi_zone.bronze import ingest_taxi_zone_lookup, ingest_taxi_zone_shapefile
from src.taxi_zone.silver1 import build as build_taxi_zone_silver1

default_args = {
    "retries": 2,
    "on_failure_callback": notify_slack_failure,
}

TAXI_ZONE_SILVER1_UPDATED = Asset("taxi_zone_silver1_updated")

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
        outlets=[TAXI_ZONE_SILVER1_UPDATED],
    )

    [lookup, shapefile] >> silver1
