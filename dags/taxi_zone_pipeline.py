"""
DAG: taxi_zone_pipeline

정적/저빈도 참조 테이블(Taxi Zone lookup + shapefile) ingestion.
스케줄 없음 (schedule=None) — 필요할 때 Airflow UI에서 수동으로 Trigger.

실제 ingestion 로직은 src/taxi_zone/bronze.py(다운로드만, 검증 없음)에 있고,
검증(필수컬럼/유니크/row-count 범위)은 src/taxi_zone/silver1.py로 옮겼다 —
Bronze는 파일이 실제로 받아졌는지만 확인한다. lookup/shapefile 둘 다 받은
뒤 build_taxi_zone_silver1 하나가 두 파일을 함께 검증하고 Silver1로 옮긴다.

Silver1 산출물을 Asset(taxi_zone)으로 내보낸다 — lion_pipeline의
build_map_zone_segment가 이걸 쓴다는 걸 Airflow UI 계보에서 보여주기
위함(다만 lion_pipeline은 분기 1회라 이 Asset을 기다리지 않고 그냥 최신
파일을 읽는다 — taxi zone이 거의 안 바뀌는 데이터라 그렇게 해도 무방하다는
팀 결정).
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

with DAG(
    dag_id="taxi_zone_pipeline",
    description="정적/저빈도 참조 테이블 (Taxi Zone lookup, shapefile) 수동 실행용",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["taxi_zone", "static"],
) as dag:

    task_lookup = PythonOperator(
        task_id="ingest_taxi_zone_lookup",
        python_callable=ingest_taxi_zone_lookup,
    )

    task_shapefile = PythonOperator(
        task_id="ingest_taxi_zone_shapefile",
        python_callable=ingest_taxi_zone_shapefile,
    )

    task_build_silver1 = PythonOperator(
        task_id="build_taxi_zone_silver1",
        python_callable=build_taxi_zone_silver1,
        outlets=[Asset("taxi_zone")],
        # bronze_root/silver1_root 둘 다 기본값(common.config 기준) 사용 —
        # lookup/shapefile 둘 다 받은 뒤에만 검증 가능하므로 두 ingest task를
        # 전부 기다린다.
    )

    [task_lookup, task_shapefile] >> task_build_silver1
