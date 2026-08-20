"""
Bronze — 공사 허가 (Street Construction Permits)

역할

- Socrata API에서 공사 허가 데이터 수집 (issuedworkstartdate 2025-01-01 이후만 —
  road_closures/stipulations와 동일하게 프로젝트에서 필요한 범위로 제한)
- API 원본을 최대한 그대로 저장
- Parquet 형식으로 날짜별 스냅샷 저장
- 동일 RUN_DATE 재실행 시 같은 경로에 저장하여 멱등성 유지

매일 이 범위 전체를 통째로 다시 받는 방식(증분 아님)이라, 이 태스크가 처음 도는
날부터 이미 2025-01-01~현재 전체가 다 받아진다 — 별도 백필 스크립트가 필요 없다
(road_closures와 동일한 이유, src/road_closures/bronze.py 참고).

※ 데이터 정제, 필터링(날짜 제외) 이외의 타입 변환 등은 수행하지 않는다.
"""

import sys
import os
from pathlib import Path
from datetime import date

import pandas as pd

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from common.config import BRONZE_DIR, DATASETS
from common.socrata import fetch_all_streaming
from common.logger import get_logger

from common.gx import validate_pandas_dataframe
from common.alerts import notify_slack_message

from construction.expectations import critical_expectations, log_only_expectations

from airflow.sdk.exceptions import AirflowSkipException


logger = get_logger(__name__, log_to_file=True, log_file_stem="construction_bronze")

SOURCE = "construction"
# permitnumber만으로는 페이지 경계에서 tie-breaker가 없어서, 같은
# permitnumber를 가진 행이 누락되거나 중복될 수 있다(construction_stipulations,
# road_closures에서 이미 겪은 문제와 동일 원인). Socrata 내부 고유 행 식별자인
# :id를 같이 걸어서 경계에서 행이 새지 않게 한다.
ORDER = "permitnumber, :id"

# 프로젝트에서 필요한 범위 (road_closures/stipulations와 동일 기준)
WHERE = "issuedworkstartdate >= '2025-01-01T00:00:00'"

# 신선도 fallback 임계값. 이보다 오래된 스냅샷은 백업으로 인정하지 않는다.
MAX_FALLBACK_AGE_DAYS = 2


def build() -> str:
    """Socrata에서 전체를 받아 저장만 한다(validate 없음) — build/validate를
    별도 Airflow 태스크로 나눠서, validate 실패로 재시도할 때 이 페이지네이션
    fetch를 처음부터 다시 안 해도 되게 하기 위함."""

    run_date = os.getenv(
        "RUN_DATE",
        date.today().isoformat(),
    )

    out_path = (
        BRONZE_DIR
        / SOURCE
        / f"dt={run_date}"
        / "data.parquet"
    )

    logger.info(
        "공사 전체 수집 시작: run_date=%s",
        run_date,
    )

    try:
        total = fetch_all_streaming(
            url=DATASETS[SOURCE],
            where=WHERE,
            order=ORDER,
            out_path=out_path,
        )

        logger.info(
            "공사 전체 수집 완료: rows=%d path=%s",
            total,
            out_path,
        )

    except Exception:
        logger.exception(
            "공사 전체 수집 실패: run_date=%s",
            run_date,
        )
        raise

    return str(out_path)


def _find_recent_bronze_snapshot(
    base_dir: Path,
    run_date: str,
    max_age_days: int = MAX_FALLBACK_AGE_DAYS,
) -> str | None:
    """base_dir 안에서 run_date보다 이전이면서 max_age_days 이내인 dt= 폴더 중
    가장 최근 것을 찾는다. data.parquet이 실제로 있는 폴더만 후보로 본다.
    """

    today = date.fromisoformat(run_date)

    candidates = []
    for entry in base_dir.iterdir():

        if not entry.is_dir() or not entry.name.startswith("dt="):
            continue

        snapshot_date_str = entry.name.removeprefix("dt=")

        try:
            snapshot_date = date.fromisoformat(snapshot_date_str)
        except ValueError:
            continue

        if snapshot_date >= today:
            continue

        if not (entry / "data.parquet").exists():
            continue

        candidates.append(snapshot_date)

    if not candidates:
        return None

    most_recent = max(candidates)

    if (today - most_recent).days > max_age_days:
        return None

    return str(base_dir / f"dt={most_recent.isoformat()}" / "data.parquet")


def validate_output(path: str) -> str:
    """저장된 Bronze 파일을 GX로 검증한다.

    critical 검증 실패 시: 최근(MAX_FALLBACK_AGE_DAYS 이내) 정상 스냅샷이
    있으면 로그+Slack 알림 후 AirflowSkipException을 던져 오늘 하루를
    조용히 건너뛴다(기존 Silver/Gold/Mapping 결과 유지 — 이 태스크가 skip되면
    Airflow 기본 trigger rule에 따라 하위 태스크 전부 자동으로 skip된다).
    없으면 일반 예외를 던져 DAG를 실패시킨다(기존 on_failure_callback 경로).
    log-only 검증 실패는 로그만 남기고 통과시킨다.
    """

    df = pd.read_parquet(path)

    critical_results = validate_pandas_dataframe(
        df,
        critical_expectations(),
        datasource_name="construction_bronze_critical",
        asset_name="construction_bronze_critical",
    )
    failed_critical = [r for r in critical_results if not r["success"]]

    if failed_critical:

        reasons = [
            f"{r['expectation_type']}({r['kwargs'].get('column', '-')})"
            for r in failed_critical
        ]

        run_date = Path(path).parent.name.removeprefix("dt=")
        fallback_path = _find_recent_bronze_snapshot(BRONZE_DIR / SOURCE, run_date)

        if fallback_path:

            logger.error(
                "공사 Bronze critical 검증 실패, 최근 스냅샷 유지 : %s (사유: %s)",
                fallback_path,
                reasons,
            )

            notify_slack_message(
                f":warning: 공사 Bronze 검증 실패, 오늘 업데이트 스킵\n"
                f"*사유*: {reasons}\n*유지되는 데이터*: `{fallback_path}`"
            )

            raise AirflowSkipException(
                f"critical 검증 실패, 최근 스냅샷으로 대체(오늘 스킵) : {reasons}"
            )

        raise ValueError(
            f"공사 Bronze critical 검증 실패, 최근 백업도 없음 : {reasons}"
        )

    log_results = validate_pandas_dataframe(
        df,
        log_only_expectations(),
        datasource_name="construction_bronze_logonly",
        asset_name="construction_bronze_logonly",
    )

    for result in log_results:

        if not result["success"]:

            logger.warning(
                "공사 Bronze 검증 실패(로그만) : %s %s → %s (exception_info: %s)",
                result["expectation_type"],
                result["kwargs"],
                result["result"],
                result["exception_info"],
            )

    return path


def main() -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    path = build()
    validate_output(path)
    return path


if __name__ == "__main__":
    main()