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

Type1(segment_metrics_type1)의 폐기 세그먼트 정리도 이 DAG가 같이 한다
(cleanup_type1_stale_segments) - Type1 자체는 30분 주기 증분 upsert라
"이번 실행 결과 = 전체 정답"이 아니어서 스왑을 못 쓰고, 세그먼트가 실제로
생기고 없어지는 시점은 어차피 LION 갱신뿐이라 이 트리거에 얹는 게 맞다
(2026-08-25, docs/superpowers/specs 참고). 비교 기준은 raw LION segment_id가
아니라 filter_routable_segments가 걸러낸 "유효 ID"다 - is_routable이
False가 되거나 length_ft가 0 이하로 바뀌어도 Type1 대상에서 빠져야
하기 때문이다(Type2와 동일한 필터를 그대로 재사용).
"""

from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.sdk import Asset

from src.common import db
from src.common.alerts import notify_slack_failure
from src.common.config import SERVING_TABLE_TYPE1, SERVING_TABLE_TYPE2
from src.lion.gold2 import build_dim_segment, validate_dim_segment
from src.nav_length.gold1 import filter_routable_segments
from src.nav_length.gold2 import to_serving_items, write_to_rds

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="segment_length_pipeline",
    description="type2(길이) — LION 세그먼트 길이를 RDS에 스냅샷 교체, type1(시간)의 폐기 세그먼트 정리 (lion_pipeline Asset 트리거)",
    schedule=Asset("lion_dim_segment_ready"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=notify_slack_failure,
    tags=["nav", "type1", "type2", "length"],
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

    @task
    def cleanup_type1_stale_segments(dim_segment_path: str) -> dict:
        """Type1(segment_metrics_type1)에서 더 이상 유효하지 않은 세그먼트의
        시간 슬롯을 전부 지운다. write_gold2_to_rds와 동일한 dim_segment_path를
        입력받아 같은 필터(filter_routable_segments)로 "지금 유효한 ID"를
        구하고, db.cleanup_keys_not_in으로 RDS 실제 상태와 직접 비교해
        수렴시킨다 - 이전 실행이 성공했는지에 의존하는 상태가 없다.
        write_gold2_to_rds와는 서로 독립이라 병렬로 돈다.

        segment_time_pipeline(Type1 30분 증분 upsert)과는 아직 실행 순서를
        강제하지 않는다 - 정리 직후 구버전 LION을 참조하던 Type1 실행이
        방금 지운 세그먼트를 다시 upsert할 여지가 이론상 남아있다. 실측상
        영향은 미미하다고 보지만(막 routable에서 빠진 세그먼트는 대체로
        실제 GPS 트래픽도 같이 없다), 엄격히 막으려면 두 파이프라인을
        같은 1-slot Airflow pool로 직렬화해야 한다(후속 과제)."""
        df = pd.read_parquet(dim_segment_path)
        valid_ids = filter_routable_segments(df)["segment_id"].tolist()
        result = db.cleanup_keys_not_in(SERVING_TABLE_TYPE1, valid_ids, "segment_id")
        return {**result, "stale_keys": result["stale_keys"][:20]}

    gold2_lion_path = build_gold2_lion()
    write_gold2_to_rds(gold2_lion_path)
    cleanup_type1_stale_segments(gold2_lion_path)


segment_length_pipeline()
