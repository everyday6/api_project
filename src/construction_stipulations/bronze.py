"""
Bronze — 공사 허가 스티퓰레이션 (Street Construction Permits - Stipulations, 2020-Present)

허가번호(permitnumber)당 여러 건 붙는 조건/유의사항 레코드다. 전체가 약 4,600만
건으로 공사 허가(약 380만 건) 대비 12배 이상 커서, construction처럼 매일 전체를
새로 받으면 실행 시간·디스크 사용량이 하루하루 부담스러워진다. 그래서 이 소스는
createdon(레코드 생성일) 기준 하루치만 받는 증분 수집으로 만든다 — RUN_DATE 하루
동안 새로 생긴 스티퓰레이션만 받아서 dt=RUN_DATE 파티션에 저장한다.

허가 하나에 스티퓰레이션이 여러 개 달리기 때문에(1:N) permitnumber만으로는
페이지네이션 정렬 기준이 유일하지 않아, 정렬에 :id(Socrata 내부 고유 행 식별자)를
같이 걸어서 페이지 경계에서 같은 permitnumber를 가진 행이 누락되지 않게 한다.

날짜 구간은 반드시 RUN_DATE(Airflow 논리 실행일, {{ ds }}) 하나만 기준으로
[RUN_DATE, RUN_DATE+1일) 구간을 직접 계산해서 쓴다. Airflow의
data_interval_start/end는 쓰지 않는다 — road_closures에서 이미 겪은 문제인데,
DAG를 수동 트리거하면 이 둘이 똑같이 "트리거 시각"으로 찌그러져서 [오늘,오늘)
같은 빈 구간이 되어버린다(실제로 84개 주간 파티션이 이 문제로 전부 0행이 됐던
사고가 있었음, src/road_closures/bronze.py 참고). RUN_DATE 하나만 쓰고 끝을
직접 +1일로 계산하면 수동/스케줄 트리거 여부와 무관하게 항상 유효한 구간이 된다.

동일 RUN_DATE 재실행 시 같은 경로에 저장하여 멱등성 유지.

주의: 이 DAG가 어느 하루 아예 안 돌면(장애/일시정지 등) 그날 생성된 레코드는
증분 수집 특성상 이후에 다시 안 받아진다 — 전체 스냅샷이 아니기 때문. 결측이
의심되면 해당 dt로 태스크를 수동 재실행(backfill)해서 메꿔야 한다.

※ 데이터 정제, 필터링, 타입 변환 등은 수행하지 않는다 (Bronze 원칙).
"""

import sys
import os
from pathlib import Path
from datetime import date, timedelta

import pandas as pd

sys.path.append(
    str(Path(__file__).resolve().parent.parent)
)

from common.config import BRONZE_DIR, DATASETS
from common.socrata import fetch_all_streaming
from common.logger import get_logger


logger = get_logger(__name__, log_to_file=True, log_file_stem="construction_stipulations_bronze")

SOURCE = "construction_stipulations"
ORDER = "permitnumber, stipulationid, :id"


def build(run_date: str | None = None) -> str | None:
    """하루치(run_date) 증분 수집. 신규 0건이면 fetch_all_streaming이 파일을
    만들지 않으므로 정상 케이스로 보고 None을 반환한다(호출부/validate_output
    둘 다 None을 "그날 신규 없음"으로 취급)."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    start = date.fromisoformat(run_date)
    end = start + timedelta(days=1)

    where = (
        f"createdon >= '{start.isoformat()}T00:00:00' "
        f"AND createdon < '{end.isoformat()}T00:00:00'"
    )

    out_path = (
        BRONZE_DIR
        / SOURCE
        / f"dt={run_date}"
        / "data.parquet"
    )

    logger.info(
        "공사 스티퓰레이션 증분 수집 시작: run_date=%s where=%s",
        run_date,
        where,
    )

    try:
        total = fetch_all_streaming(
            url=DATASETS[SOURCE],
            where=where,
            order=ORDER,
            out_path=out_path,
        )

        # construction(전체 스냅샷)과 달리 여기서는 0건이 비정상이 아니다 — 특정
        # 하루에 신규 스티퓰레이션이 없거나(주말/공휴일), 소스 반영이 지연될 수
        # 있다(실제로 지금 소스 최신 createdon이 오늘보다 하루 이상 뒤처져 있음을
        # 확인함). fetch_all_streaming도 0건을 정상 케이스로 보고 출력 파일을
        # 만들지 않으므로, 여기서 에러로 취급하지 않고 로그만 남긴다.
        if total == 0:
            logger.warning(
                "공사 스티퓰레이션 신규 건수 0건: run_date=%s (정상일 수 있음 — "
                "소스 반영 지연 또는 실제로 해당일 신규 레코드 없음)",
                run_date,
            )
            return None

        logger.info(
            "공사 스티퓰레이션 증분 수집 완료: rows=%d path=%s",
            total,
            out_path,
        )
        return str(out_path)

    except Exception:
        logger.exception(
            "공사 스티퓰레이션 증분 수집 실패: run_date=%s",
            run_date,
        )
        raise


def validate_output(path: str | None) -> str | None:
    """path가 None이면(그날 신규 0건이라 build()가 파일을 안 만든 경우) 정상
    케이스이니 그대로 통과시킨다. 파일이 있으면 permitnumber 컬럼이 비어있지
    않은지만 확인한다(Bronze라 그 이상의 정제/검증은 하지 않음)."""
    if path is None:
        logger.info("공사 스티퓰레이션 검증 스킵: 그날 신규 0건(정상 케이스)")
        return path

    df = pd.read_parquet(path, columns=["permitnumber"])
    if df.empty:
        raise ValueError(f"공사 스티퓰레이션 파일이 비어 있습니다: {path}")

    logger.info("공사 스티퓰레이션 검증 완료: rows=%d path=%s", len(df), path)
    return path


def main(run_date: str | None = None) -> str | None:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    path = build(run_date)
    return validate_output(path)


if __name__ == "__main__":
    main()
