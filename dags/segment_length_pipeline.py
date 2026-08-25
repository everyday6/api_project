"""
DAG: segment_length_pipeline (type2 — 길이)

LION 세그먼트 길이를 RDS(segment_metrics_type2)에 upsert한다.

LION 원본 다운로드/정제(Silver1 dim_segment)는 lion_pipeline이 담당한다 -
예전엔 이 파이프라인도 독자적으로 같은 LION zip을 받아 같은
dim_segment.parquet를 만들었는데(중복), 이제 lion_pipeline이 Silver1을
발행할 때 emit하는 lion_dim_segment_ready Asset에 반응해서 is_routable
계산(Gold2)과 RDS 반영만 한다.

LION 파싱은 이제 필요 없어 GDAL 의존이 사라졌고, Gold2 계산도 세그먼트당
값 하나(10~30만 행)뿐이라 EMR Serverless를 쓸 만큼 큰 데이터가 아니다 -
Airflow 워커 안에서 pandas로 직접 처리한다(2026-08-25 Spark/EMR ->
pandas 전환. write_to_rds/to_serving_items는 애초에 결과를 driver로
collect해서 순수 파이썬으로 처리하고 있었어서, 실질적으로는 이미
pandas나 다름없었다).
"""

from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.sdk import Asset

from src.common.alerts import notify_slack_failure
from src.common.config import SERVING_TABLE_TYPE2
from src.lion.gold2 import build_dim_segment, validate_dim_segment
from src.nav_length.gold1 import filter_routable_segments
from src.nav_length.gold2 import to_serving_items, write_to_rds

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="segment_length_pipeline",
    description="type2(길이) — LION 세그먼트 길이를 RDS에 upsert (lion_pipeline Asset 트리거)",
    schedule=Asset("lion_dim_segment_ready"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=notify_slack_failure,
    tags=["nav", "type2", "length"],
)
def segment_length_pipeline():

    @task
    def build_gold2_lion() -> str:
        path = build_dim_segment()
        return validate_dim_segment(path)

    @task
    def write_gold2_to_rds(dim_segment_path: str) -> int:
        df = pd.read_parquet(dim_segment_path)
        filtered = filter_routable_segments(df)
        items = to_serving_items(filtered)
        return write_to_rds(items, SERVING_TABLE_TYPE2)

    gold2_lion_path = build_gold2_lion()
    write_gold2_to_rds(gold2_lion_path)


segment_length_pipeline()
