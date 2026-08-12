"""
DAG: ingest_weekly

도로 통제(road_closures) ingestion. 매주 월요일 새벽에 실행되며, BACKFILL_START
(2025-01-01)부터 이번 실행일까지 전체를 매번 통째로 다시 받아 하나의 parquet
파일로 저장한다 (더 이상 주 단위로 쪼개서 증분 저장하지 않음).

"이번 실행일"은 data_interval이 아니라 논리적 실행일({{ ds }})만 쓴다 — 예전엔
data_interval_start/end로 "이번 주 구간"을 계산했는데, DAG를 수동 트리거하면
Airflow가 이 둘을 똑같이 "트리거 시각"으로 채워서 [오늘,오늘) 같은 빈 구간이
되는 버그가 있었다(실제로 이 버그로 84주치 데이터가 전부 0행으로 덮어써짐).
지금 방식은 시작일이 고정값(BACKFILL_START)이라 end_date가 뭐가 되든 항상
유효한 구간이 되고, 수동으로 몇 번을 다시 돌려도 매번 최신 전체 스냅샷을
새로 받아오는 것뿐이라 안전하다.

실제 ingestion/검증 로직은 src/road_closures/bronze.py에 있고, 이 파일은
언제/어떤 순서로 그 함수들을 실행할지만 정의한다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.road_closures.bronze import ingest_road_closures, validate_road_closures

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingest_weekly",
    description="도로 통제(road_closures) 전체 스냅샷 갱신 (BACKFILL_START ~ 실행일)",
    schedule="0 4 * * 1",          # 매주 월요일 새벽 4시
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bronze", "weekly"],
) as dag:

    task_ingest_road_closures = PythonOperator(
        task_id="ingest_road_closures",
        python_callable=ingest_road_closures,
        op_kwargs={
            # 수동/스케줄 트리거 관계없이 "이번 실행일까지 전체"를 받는다.
            "end_date": "{{ ds }}",
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
