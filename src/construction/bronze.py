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

# validate_output()이 이 날짜의 검증을 통과시켰다는 표시. data.parquet은
# critical 검증에 실패한 날에도 이미 저장돼 있으므로, 신선도 fallback의
# "최근 정상 스냅샷" 판정은 파일 존재가 아니라 이 마커로 해야 한다 — 안 그러면
# 검증에 실패한 날의 파일이 그 다음 날의 "백업"이 되어, 2일 초과 시 실패로
# 확실히 승격돼야 할 상황에서도 영원히 조용히 스킵만 반복하게 된다.
VALIDATED_MARKER_NAME = "_VALIDATED"


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
    """base_dir 안에서 run_date보다 이전이면서 max_age_days 이내인, 검증을
    통과한(= VALIDATED_MARKER_NAME이 있는) dt= 폴더 중 가장 최근 것을 찾는다.
    """

    if not base_dir.exists():
        return None

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

        if not (entry / VALIDATED_MARKER_NAME).exists():
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


def _handle_critical_failure(reasons: list, path: str) -> None:
    """critical 검증 실패(또는 Bronze 파일 자체가 없음)를 신선도 fallback
    정책에 따라 처리한다. 항상 예외를 던지며 정상적으로 반환하지 않는다 —
    최근(MAX_FALLBACK_AGE_DAYS 이내) 검증 통과 스냅샷이 있으면 로그+Slack
    알림 후 AirflowSkipException을 던져 오늘 하루를 조용히 건너뛰고(하위
    태스크 전부 자동 skip), 없으면 일반 예외를 던져 DAG를 실패시킨다(기존
    on_failure_callback 경로).
    """

    run_date = Path(path).parent.name.removeprefix("dt=")
    fallback_path = _find_recent_bronze_snapshot(BRONZE_DIR / SOURCE, run_date)

    if fallback_path:

        logger.error(
            "공사 Bronze critical 검증 실패, 최근 검증 통과 스냅샷 유지 : %s (사유: %s)",
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


def validate_output(path: str) -> str:
    """저장된 Bronze 파일을 GX로 검증한다.

    Bronze 파일 자체가 없거나(Socrata가 0건을 반환해 build()가 아무것도
    저장하지 못한 경우 포함) critical 검증에 실패하면 _handle_critical_failure로
    처리를 위임한다. log-only 검증 실패는 로그만 남기고 통과시킨다. 정상
    통과 시(log-only 실패 포함) VALIDATED_MARKER_NAME 마커를 남겨, 이 날짜가
    이후 신선도 fallback의 후보가 될 수 있게 한다.
    """

    if not Path(path).exists():
        _handle_critical_failure([f"bronze_file_missing({path})"], path)

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
        _handle_critical_failure(reasons, path)

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

    (Path(path).parent / VALIDATED_MARKER_NAME).touch()

    return path


def main() -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    path = build()
    validate_output(path)
    return path


if __name__ == "__main__":
    main()