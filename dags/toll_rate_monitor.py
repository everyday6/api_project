"""
DAG: toll_rate_monitor

통행료 요금표를 매달 확인하라고 사람에게 알려주기만 하는 DAG. mta.info가
봇 차단(WAF)이 걸려있어서 페이지 내용을 코드로 가져와 비교하는 자동 변경
감지는 불가능하다고 확인했다(src/toll/rate_monitor.py 모듈 docstring
참고) — 그래서 매달 무조건 Slack 알림을 보내고, 실제 확인/판단/
config/toll_rates.yaml 수정은 항상 사람이 한다.
"""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from src.common.alerts import notify_slack_failure, notify_slack_message

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": notify_slack_failure,
}


@dag(
    dag_id="toll_rate_monitor",
    description="통행료 요금표 월간 확인 알림 (자동 변경 감지 아님 — mta.info 봇 차단)",
    schedule="0 9 1 * *",  # 매달 1일 오전 9시
    start_date=pendulum.datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["toll", "monthly"],
)
def toll_rate_monitor():

    @task(task_id="send_reminder")
    def send_reminder():
        from src.toll.rate_monitor import build_reminder_message

        notify_slack_message(build_reminder_message())

    send_reminder()


toll_rate_monitor()
