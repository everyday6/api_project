"""
DAG: taxi_zone_pipeline

정적/저빈도 참조 테이블(Taxi Zone lookup + shapefile) ingestion.
스케줄 없음 (schedule=None) — 필요할 때 Airflow UI에서 수동으로 Trigger.

실제 ingestion/검증 로직은 src/taxi_zone/bronze.py에 있고, 이 파일은 그 함수들을
언제/어떤 task로 실행할지만 정의한다. lookup/shapefile 각각 build >> validate로
분리해서, validate 실패 시 재시도할 때 다운로드부터 다시 하지 않아도 되게 한다.

shapefile을 Asset(taxi_zone)으로 내보낸다 — lion_pipeline의 build_map_zone_segment가
이걸 쓴다는 걸 Airflow UI 계보에서 보여주기 위함(다만 lion_pipeline은 분기
1회라 이 Asset을 기다리지 않고 그냥 최신 파일을 읽는다 — taxi zone이 거의
안 바뀌는 데이터라 그렇게 해도 무방하다는 팀 결정).
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Asset

from src.taxi_zone.bronze import (
    ingest_taxi_zone_lookup,
    ingest_taxi_zone_shapefile,
    validate_taxi_zone_lookup,
    validate_taxi_zone_shapefile,
)

default_args = {
    "retries": 2,
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

    task_validate_lookup = PythonOperator(
        task_id="validate_taxi_zone_lookup",
        python_callable=validate_taxi_zone_lookup,
        op_kwargs={
            "path": "{{ ti.xcom_pull(task_ids='ingest_taxi_zone_lookup') }}",
        },
    )

    task_shapefile = PythonOperator(
        task_id="ingest_taxi_zone_shapefile",
        python_callable=ingest_taxi_zone_shapefile,
        outlets=[Asset("taxi_zone")],
    )

    task_validate_shapefile = PythonOperator(
        task_id="validate_taxi_zone_shapefile",
        python_callable=validate_taxi_zone_shapefile,
        op_kwargs={
            "path": "{{ ti.xcom_pull(task_ids='ingest_taxi_zone_shapefile') }}",
        },
    )

    # lookup/shapefile 두 체인은 서로 의존관계 없음 -> 병렬 실행
    task_lookup >> task_validate_lookup
    task_shapefile >> task_validate_shapefile
