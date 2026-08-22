"""
통행료 요금표 월간 확인 알림

원래는 공식 요금 페이지 내용을 가져와 해시로 비교해서 "바뀐 것 같다"를
자동 판단하려 했으나, 실제로 해보니 mta.info가 봇 차단(WAF)이 걸려있어서
requests/curl 어떤 방식(브라우저 User-Agent 포함)으로도 페이지를 가져올
수 없다(403 Access Denied). 그래서 자동 변경 감지는 포기하고, 매달
무조건 "직접 확인하라"는 알림만 보낸다 — 확인/판단/config/toll_rates.yaml
수정은 항상 사람이 한다.
"""

from __future__ import annotations

RATE_PAGE_URLS = [
    "https://www.mta.info/fares-tolls/tolls/vehicle-types",
    "https://www.mta.info/fares-tolls/tolls/congestion-relief-zone",
    "https://www.panynj.gov/bridges-tunnels/en/e-zpass.html",
]


def build_reminder_message(urls: list[str] = RATE_PAGE_URLS) -> str:
    """매달 보낼 Slack 알림 메시지를 만든다."""

    urls_text = "\n".join(f"- {url}" for url in urls)
    return (
        ":bell: 통행료 요금표 월간 확인 알림\n"
        f"{urls_text}\n"
        "위 페이지들을 직접 확인해서 config/toll_rates.yaml과 다르면 고친 뒤 "
        "toll_bronze_pipeline DAG를 수동 트리거하세요."
    )
