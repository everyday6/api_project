"""
DAG: segment_length_pipeline (type2 — 길이)

LION 세그먼트 길이를 RDS(segment_metrics_type2)에 upsert한다.

LION 원본 다운로드/정제(Silver1 dim_segment)는 lion_pipeline이 담당한다 -
예전엔 이 파이프라인도 독자적으로 같은 LION zip을 받아 같은
dim_segment.parquet를 만들었는데(중복), 이제 lion_pipeline이 Silver1을
발행할 때 emit하는 lion_dim_segment_ready Asset에 반응해서 is_routable
계산(Gold2)과 RDS 반영만 한다.

LION 파싱은 이제 필요 없어 GDAL 의존이 사라졌고, Gold2/EMR 제출만 남는다.
"""

import uuid
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sdk import Asset

from src.common.alerts import notify_slack_failure
from src.common.config import EMR_JOBS_DIR, PROJECT_ROOT, SERVING_TABLE_TYPE2
from src.common.emr_serverless import read_json_result, run_spark_job
from src.lion.gold2 import build_dim_segment, validate_dim_segment

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

    @task(pool="silver_pool")
    def submit_nav_length_job(dim_segment_path: str) -> dict:
        run_id = uuid.uuid4().hex
        output_s3 = EMR_JOBS_DIR / "outputs" / f"nav_length_{run_id}.json"

        run_spark_job(
            job_name=f"nav-length-{run_id}",
            entry_point_script=PROJECT_ROOT / "spark_jobs" / "nav_length_job.py",
            entry_point_args=[
                "--dim-segment-path", dim_segment_path,
                "--serving-table", SERVING_TABLE_TYPE2,
                "--output-s3", str(output_s3),
            ],
        )

        return read_json_result(str(output_s3))

    gold2_lion_path = build_gold2_lion()
    submit_nav_length_job(gold2_lion_path)


segment_length_pipeline()
