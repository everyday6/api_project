"""
DAG: toll_silver_gold_pipeline

toll_bronze_pipeline이 요금표/시설목록/CBD 폴리곤을 갱신하거나
(Asset("toll_bronze_updated")), lion_pipeline이 분기 LION을 갱신할 때
(Asset("lion_bronze_updated")) Silver2 매핑(lion_facility, lion_cbd)을
다시 만들고 Gold 값을 재계산해서 RDS에 적재한다. 이름을
"gold_pipeline"이 아니라 "silver_gold_pipeline"으로 붙인 이유는 실제로
Silver2 매핑 태스크가 여기 포함돼 있어서다(순수 Gold 계산만 하는 DAG가
아님). 요금표가 1년에 한 번 정도만 바뀌므로 cron 스케줄 없이 Asset
트리거만 쓴다(gold_closure_penalty와 동일한 패턴).

두 Asset을 리스트로 넘기면 Airflow는 AND로 해석해서 "둘 다" 갱신돼야
트리거한다(둘 중 하나만 바뀌어도 반응해야 하는 이 상황엔 안 맞음) —
그래서 리스트 대신 `|` 연산자로 OR 조건을 명시한다.
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


@dag(
    dag_id="toll_silver_gold_pipeline",
    description="통행료 Silver2 매핑(lion_facility, lion_cbd) + Gold 계산 (toll_bronze_pipeline 또는 lion_pipeline Asset 트리거)",
    schedule=Asset("toll_bronze_updated") | Asset("lion_bronze_updated"),
    start_date=pendulum.datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["toll", "asset-triggered"],
)
def toll_silver_gold_pipeline():

    @task(task_id="find_latest_lion_gdb")
    def find_latest_lion_gdb() -> str:
        from src.common.config import BRONZE_DIR

        gdb_candidates = sorted((BRONZE_DIR / "lion").glob("version_date=*/lion/lion.gdb"))
        if not gdb_candidates:
            raise FileNotFoundError("LION Bronze GDB를 찾을 수 없습니다 — lion_pipeline DAG를 먼저 실행하세요.")
        return str(gdb_candidates[-1])

    @task(task_id="build_lion_facility_mapping")
    def build_lion_facility_mapping_task(gdb_path: str) -> str:
        from src.toll.silver2 import build_lion_facility_mapping

        # gdb_path는 S3 경로일 수 있다(예: "s3://bucket/...") — stdlib
        # pathlib.Path로 감싸면 "//"가 "/"로 뭉개져서 s3:／bucket/... 이 돼버려
        # 더 이상 유효한 S3 URI가 아니게 된다(실제로 겪음, build_and_write_gold
        # FileNotFoundError 원인). 문자열 그대로 넘긴다.
        return build_lion_facility_mapping(gdb_path=gdb_path)

    @task(task_id="build_lion_cbd_mapping")
    def build_lion_cbd_mapping_task(gdb_path: str) -> str:
        from src.toll.silver2 import build_lion_cbd_mapping

        return build_lion_cbd_mapping(gdb_path=gdb_path)

    @task(task_id="build_and_write_gold")
    def build_and_write_gold(lion_facility_map_path: str, lion_cbd_map_path: str) -> int:
        from src.toll.gold import build_and_write

        return build_and_write(
            lion_facility_map_path=lion_facility_map_path,
            lion_cbd_map_path=lion_cbd_map_path,
        )

    gdb_path = find_latest_lion_gdb()
    lion_facility_map = build_lion_facility_mapping_task(gdb_path)
    lion_cbd_map = build_lion_cbd_mapping_task(gdb_path)
    build_and_write_gold(lion_facility_map, lion_cbd_map)


toll_silver_gold_pipeline()
