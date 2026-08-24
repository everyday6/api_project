"""
DAG: segment_length_pipeline (type2 — 길이 / type1 SPEC Estimate)

LION 세그먼트 길이를 RDS(segment_metrics_type2)에 upsert한다.

LION 원본 다운로드/정제(Silver1 dim_segment)는 lion_pipeline이 담당한다 -
예전엔 이 파이프라인도 독자적으로 같은 LION zip을 받아 같은
dim_segment.parquet를 만들었는데(중복), 이제 lion_pipeline이 Silver1을
발행할 때 emit하는 lion_dim_segment_ready Asset에 반응해서 is_routable
계산(Gold2)과 RDS 반영만 한다.

LION 파싱은 이제 필요 없어 GDAL 의존이 사라졌고, Gold2/EMR 제출만 남는다.

build_and_write_spec_estimates(type1의 3단계 SPEC Estimate 폴백, 실시간
속도 데이터가 아니라 LION 도로 스펙만 씀 — type2(segment_metrics_type2)와는
별개 테이블인 segment_metrics_type1에 씀)도 여기서 같이 처리한다 - "하나의
데이터 - 하나의 DAG" 원칙은 산출물이 아니라 트리거 이유를 기준으로 하는데
(다른 nav DAG들과 동일한 근거), SPEC은 길이와 똑같이 LION이 갱신될 때만
다시 계산하면 되는 정적값이라 이 DAG의 Asset 트리거를 그대로 재사용하는 게
맞다. 실시간 30분 버킷 계산(segment_time_pipeline)과는 트리거 자체가 달라
그쪽엔 안 넣는다. 순수 pandas 연산(EMR 불필요)이라 Airflow worker에서
바로 돈다.
"""

import uuid
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.sdk import Asset

from src.common.alerts import notify_slack_failure
from src.common.config import EMR_JOBS_DIR, PROJECT_ROOT, RDS_TABLE_TYPE1, RDS_TABLE_TYPE2
from src.common.emr_serverless import read_json_result, run_spark_job
from src.lion.gold2 import build_dim_segment, validate_dim_segment
from src.nav_time.gold2 import compute_spec_travel_seconds, spec_estimate_items
from src.nav_time.gold2 import write_to_rds as write_type1_to_rds

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="segment_length_pipeline",
    description="type2(길이, RDS) + type1 SPEC Estimate(RDS) — LION 정적 스펙 upsert (lion_pipeline Asset 트리거)",
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
    def submit_nav_length_job(dim_segment_path: str) -> dict:
        run_id = uuid.uuid4().hex
        output_s3 = EMR_JOBS_DIR / "outputs" / f"nav_length_{run_id}.json"

        run_spark_job(
            job_name=f"nav-length-{run_id}",
            entry_point_script=PROJECT_ROOT / "spark_jobs" / "nav_length_job.py",
            entry_point_args=[
                "--dim-segment-path", dim_segment_path,
                "--rds-table", RDS_TABLE_TYPE2,
                "--output-s3", str(output_s3),
            ],
        )

        return read_json_result(str(output_s3))

    @task
    def build_and_write_spec_estimates(dim_segment_path: str) -> int:
        dim_segment_df = pd.read_parquet(dim_segment_path)
        items = spec_estimate_items(compute_spec_travel_seconds(dim_segment_df))
        return write_type1_to_rds(items, RDS_TABLE_TYPE1)

    gold2_lion_path = build_gold2_lion()
    submit_nav_length_job(gold2_lion_path)
    build_and_write_spec_estimates(gold2_lion_path)


segment_length_pipeline()
