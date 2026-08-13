"""
ingest_daily — 공사 · 행사 · TicketMaster 일일 파이프라인

네 소스는 매일 갱신한다.

- construction: 공사 허가 전체 최신 스냅샷 수집
- construction_stipulations: 공사 허가 스티퓰레이션(조건/유의사항) 증분 수집
  (createdon 기준 실행일 하루치만, Bronze만, Silver는 아직 없음)
- event: NYC Permitted Event 최신 상태 수집
- ticketmaster: 앞으로 120일 이벤트 수집

각 소스의 Bronze → Silver는 순차 실행하고,
소스끼리는 서로 독립적으로 병렬 실행한다.

실패 시 최대 3회 자동 재시도하며,
동일 RUN_DATE 재실행 시 동일 날짜 파티션을 사용한다.
"""

import sys
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# 프로젝트 루트를 import 경로에 추가

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))


# ==========================
# Airflow 공통 설정
# ==========================

LOCAL_TZ = pendulum.timezone("America/New_York")

default_args = {
    "owner": "de5",

    # Task 실패 시 최대 3회 자동 재시도
    "retries": 3,

    # 최초 재시도 대기 시간
    "retry_delay": timedelta(minutes=5),

    # 재시도할수록 대기 시간을 증가시킴
    "retry_exponential_backoff": True,

    # Task가 비정상적으로 오래 실행되는 것을 방지
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="ingest_daily",
    description="공사 · 행사 · TicketMaster Bronze/Silver 일일 파이프라인",

    # 매일 New York 시간 기준 오전 4시 실행
    schedule="0 4 * * *",

    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz=LOCAL_TZ,
    ),

    # 과거 미실행 날짜를 자동으로 소급 실행하지 않음
    catchup=False,

    # 동일 DAG가 동시에 여러 번 실행되는 것을 방지
    max_active_runs=1,

    default_args=default_args,

    tags=[
        "daily",
        "bronze",
        "silver",
    ],
)
def ingest_daily():

    # ───────────────────────────
    # Construction
    # ───────────────────────────

    @task(task_id="construction_bronze")
    def construction_bronze():
        from src.construction.bronze import main

        context = get_current_context()

        # Airflow 논리 실행일을 각 스크립트에 전달
        os.environ["RUN_DATE"] = context["ds"]

        main()


    @task(task_id="construction_silver")
    def construction_silver():
        from src.construction.silver import main

        context = get_current_context()
        os.environ["RUN_DATE"] = context["ds"]

        main()


    # ───────────────────────────
    # Construction Stipulations
    # ───────────────────────────

    @task(task_id="construction_stipulations_bronze")
    def construction_stipulations_bronze():
        from src.construction_stipulations.bronze import main

        context = get_current_context()

        # Airflow 논리 실행일을 각 스크립트에 전달
        os.environ["RUN_DATE"] = context["ds"]

        main()


    # ───────────────────────────
    # NYC Event
    # ───────────────────────────

    @task(task_id="event_bronze")
    def event_bronze(**context):
        from src.event.bronze import main
        main(run_date=context["ds"])


    @task(task_id="event_silver")
    def event_silver(**context):
        from src.event.silver import main
        main(run_date=context["ds"])


    # ───────────────────────────
    # TicketMaster
    # ───────────────────────────

    @task(task_id="ticketmaster_bronze")
    def ticketmaster_bronze():
        from src.ticketmaster.bronze import main

        context = get_current_context()
        os.environ["RUN_DATE"] = context["ds"]

        main()


    @task(task_id="ticketmaster_silver")
    def ticketmaster_silver():
        from src.ticketmaster.silver import main

        context = get_current_context()
        os.environ["RUN_DATE"] = context["ds"]

        main()


    # ───────────────────────────
    # Task 생성
    # ───────────────────────────

    construction_b = construction_bronze()
    construction_s = construction_silver()

    construction_stipulations_b = construction_stipulations_bronze()

    event_b = event_bronze()
    event_s = event_silver()

    ticketmaster_b = ticketmaster_bronze()
    ticketmaster_s = ticketmaster_silver()


    # ───────────────────────────
    # 의존 관계
    # ───────────────────────────

    # 각 Bronze가 성공해야 해당 Silver 실행
    construction_b >> construction_s
    event_b >> event_s
    ticketmaster_b >> ticketmaster_s


ingest_daily()