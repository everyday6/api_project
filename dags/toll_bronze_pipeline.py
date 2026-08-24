"""
DAG: toll_bronze_pipeline

통행료 요금표/시설목록/CBD 폴리곤을 Bronze에 올린다. 요금표는 사람이
config/toll_rates.yaml을 고친 뒤에만 값이 바뀌므로 cron 스케줄이 아니라
수동 트리거(schedule=None)로 둔다 — 요금표 변경 여부는 사람이 직접
공식 페이지를 확인해서 파일을 고치고 이 DAG를 수동으로 실행한다(월간
확인 알림 DAG는 mta.info 봇 차단으로 자동 감지가 불가능해 실효성이
없어 제거함).

이 DAG가 끝나면 Asset("toll_bronze_updated")을 내보내서
toll_silver_gold_pipeline이 자동으로 이어서 돈다.
"""

from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, task

from src.common.alerts import notify_slack_failure

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}

TOLL_BRONZE_UPDATED = Asset("toll_bronze_updated")


@dag(
    dag_id="toll_bronze_pipeline",
    description="통행료 요금표/시설목록/CBD 폴리곤 Bronze 업로드 (수동 트리거)",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["toll"],
)
def toll_bronze_pipeline():

    @task(task_id="upload_rates")
    def upload_rates_task():
        from src.toll.bronze import upload_rates
        return str(upload_rates())

    @task(task_id="upload_facilities")
    def upload_facilities_task():
        from src.toll.bronze import upload_facilities
        return str(upload_facilities())

    @task(task_id="upload_cbd_geofence")
    def upload_cbd_geofence_task():
        from src.toll.bronze import upload_cbd_geofence
        return str(upload_cbd_geofence())

    @task(task_id="publish_toll_bronze", outlets=[TOLL_BRONZE_UPDATED])
    def publish_toll_bronze(
        rates_path: str,
        facilities_path: str,
        cbd_geofence_path: str,
    ) -> dict:
        """세 Bronze 입력이 모두 성공한 경우에만 갱신 Asset을 발행한다."""

        return {
            "rates_path": rates_path,
            "facilities_path": facilities_path,
            "cbd_geofence_path": cbd_geofence_path,
        }

    rates = upload_rates_task()
    facilities = upload_facilities_task()
    cbd_geofence = upload_cbd_geofence_task()
    publish_toll_bronze(rates, facilities, cbd_geofence)


toll_bronze_pipeline()
