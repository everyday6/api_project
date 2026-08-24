"""
DAG: segment_time_pipeline (type1 — 시간)

NYC DOT 실시간 속도 데이터를 30분마다 수집해서, LION 세그먼트별 30분 버킷
평균 통행시간을 계산해 RDS(segment_metrics_type1)에 upsert한다(설계
문서 8절). DynamoDB에서 RDS로 옮기며 생긴 가용성 손실을 보완하려고
Gold job이 성공할 때마다 S3 Gold 스냅샷도 같이 갱신한다(src/common/gold_snapshot.py,
src/serving/nav_lookup.py 참고).

Bronze(수집)만 Airflow worker에서 돌고, Silver1~Gold2는 하나의 EMR
Serverless Spark job으로 묶어서 제출한다.
"""

import logging
import uuid
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from src.common.alerts import notify_slack_failure
from src.common.config import EMR_JOBS_DIR, PROJECT_ROOT, SERVING_TABLE_TYPE1
from src.common.emr_serverless import read_json_result, run_spark_job
from src.lion.gold2 import DIM_SEGMENT_PATH
from src.speed.bronze import collect_speed_data, has_new_speed_data
from src.speed.bronze_validation import validate_bronze

logger = logging.getLogger(__name__)

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="segment_time_pipeline",
    description="type1(시간) — NYC DOT 속도 데이터를 세그먼트별 통행시간으로 변환",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=notify_slack_failure,
    tags=["nav", "type1", "time"],
)
def segment_time_pipeline():

    @task.short_circuit
    def check_new_data() -> bool:
        return has_new_speed_data()

    @task
    def collect_bronze() -> str:
        return collect_speed_data()

    @task.short_circuit
    def check_dim_segment_exists() -> bool:
        """segment_length_pipeline이 1월/7월에만 도는 dim_segment.parquet에
        이 파이프라인(30분마다)이 매번 의존한다. 그 파일이 아직 없으면(최초
        부트스트랩 전, 또는 두 스케줄 사이 기간) EMR job이 매번 크래시하는
        대신 여기서 건너뛴다 - 부트스트랩 절차는 설계 문서 8절 참고."""
        exists = DIM_SEGMENT_PATH.exists()
        if not exists:
            logger.warning(
                "%s 없음 - segment_length_pipeline이 아직 dim_segment를 만들지 "
                "않았거나 수동 부트스트랩이 필요함. 이번 실행은 EMR job 제출을 "
                "건너뛴다.",
                DIM_SEGMENT_PATH,
            )
        return exists

    @task
    def submit_silver_job(speed_bronze_path: str) -> dict:
        run_id = uuid.uuid4().hex
        silver2_path = EMR_JOBS_DIR / "outputs" / f"nav_time_silver2_{run_id}.parquet"
        output_s3 = EMR_JOBS_DIR / "outputs" / f"nav_time_silver_{run_id}.json"

        run_spark_job(
            job_name=f"nav-time-silver-{run_id}",
            entry_point_script=PROJECT_ROOT / "spark_jobs" / "nav_time_silver_job.py",
            entry_point_args=[
                "--speed-bronze-path", speed_bronze_path,
                "--dim-segment-path", str(DIM_SEGMENT_PATH),
                "--silver2-output", str(silver2_path),
                "--output-s3", str(output_s3),
            ],
        )

        result = read_json_result(str(output_s3))
        return {"silver2_path": str(silver2_path), **result}

    @task
    def submit_gold_job(silver_result: dict) -> dict:
        run_id = uuid.uuid4().hex
        output_s3 = EMR_JOBS_DIR / "outputs" / f"nav_time_gold_{run_id}.json"

        run_spark_job(
            job_name=f"nav-time-gold-{run_id}",
            entry_point_script=PROJECT_ROOT / "spark_jobs" / "nav_time_gold_job.py",
            entry_point_args=[
                "--silver2-path", silver_result["silver2_path"],
                "--dim-segment-path", str(DIM_SEGMENT_PATH),
                "--serving-table", SERVING_TABLE_TYPE1,
                "--output-s3", str(output_s3),
            ],
        )

        return read_json_result(str(output_s3))

    new_data = check_new_data()
    bronze_path = collect_bronze()
    bronze_path.set_upstream(new_data)

    bronze_valid = validate_bronze(bronze_path)

    dim_segment_ready = check_dim_segment_exists()

    silver_result = submit_silver_job(bronze_path)
    silver_result.set_upstream(dim_segment_ready)
    silver_result.set_upstream(bronze_valid)

    gold_result = submit_gold_job(silver_result)


segment_time_pipeline()
