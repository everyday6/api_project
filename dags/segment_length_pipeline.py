"""
DAG: segment_length_pipeline (type2 — 길이)

LION 도로망에서 세그먼트별 길이를 뽑아 DynamoDB(SegmentMetricsType2)에
upsert한다. 6개월 주기(LION 정식 릴리즈 주기)로 스케줄하되, 매번 확인해서
신규 릴리즈가 없으면 나머지 태스크를 건너뛴다(설계 문서 8절).

LION 파싱(ogr2ogr)은 GDAL CLI 의존이라 Airflow worker에서 돌리고, 순수
필터/포맷 연산(Gold1/Gold2)만 EMR Serverless Spark job으로 제출한다.
"""

import uuid
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from src.common.alerts import notify_slack_failure
from src.common.config import DYNAMODB_TABLE_TYPE2, EMR_JOBS_DIR, PROJECT_ROOT
from src.common.emr_serverless import read_json_result, run_spark_job
from src.lion.bronze import check_new_lion_release, ingest_lion
from src.lion.gold2 import build_dim_segment, validate_dim_segment
from src.lion.silver1 import build_dim_segment_base, validate_dim_segment_base

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="segment_length_pipeline",
    description="type2(길이) — LION 세그먼트 길이를 DynamoDB에 upsert",
    schedule="0 5 1 1,7 *",  # 1월/7월 1일 새벽 5시 (LION 반년 릴리즈 주기에 맞춤)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=notify_slack_failure,
    tags=["nav", "type2", "length"],
)
def segment_length_pipeline():

    @task.short_circuit
    def check_new_release() -> bool:
        return check_new_lion_release()

    @task
    def ingest() -> str:
        # {{ ds }}를 넘기지 않는다 - 수동 트리거(logical_date 없음)에서
        # Jinja가 UndefinedError로 죽는다. ingest_lion()은 인자 없으면
        # 실행 시점의 실제 날짜로 태깅한다.
        return ingest_lion()

    @task
    def build_silver1(_bronze_path: str) -> str:
        path = build_dim_segment_base()
        return validate_dim_segment_base(path)

    @task
    def build_gold2_lion(_silver1_path: str) -> str:
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
                "--dynamodb-table", DYNAMODB_TABLE_TYPE2,
                "--output-s3", str(output_s3),
            ],
        )

        return read_json_result(str(output_s3))

    new_release = check_new_release()
    bronze_path = ingest()
    bronze_path.set_upstream(new_release)

    silver1_path = build_silver1(bronze_path)
    gold2_lion_path = build_gold2_lion(silver1_path)
    submit_nav_length_job(gold2_lion_path)


segment_length_pipeline()
