"""
TLC 다운로드 검증 Task

역할
1. 다운로드 파일이 존재하고 비어있지 않은지 확인(src/common/file_validation.py)
2. 검증된 다운로드 정보를 dict 형태로 다음 Task에 전달

"공통 검증 모듈"이라는 이전 이름과 달리 실제로는 tlc_ingest_pipeline만 쓰는
TLC 전용 Airflow task다(logger stem도 원래부터 "tlc_bronze") - 형식 검증
로직 자체는 src/common/file_validation.py로 옮겨서 다른 도메인(LION,
Toll)도 재사용하고, 여긴 TLC DAG용 task 래퍼 역할만 한다.
"""

from pathlib import Path

from airflow.decorators import task

from src.common.file_validation import validate_non_empty
from src.common.logger import get_logger


# Logger 생성
logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_bronze")


@task
def validate_download(
    download_result: dict,
) -> dict:
    """다운로드된 파일을 검증한다."""

    # -----------------------------------------
    # 다운로드 결과에서 필요한 정보 추출
    # -----------------------------------------

    filename = download_result["filename"]

    # XCom으로 전달된 경로는 문자열이므로
    # 실제 파일 경로 객체로 변환
    tmp_path = Path(
        download_result["tmp_path"]
    )

    # -----------------------------------------
    # 파일 존재 + 비어있지 않은지 확인
    # -----------------------------------------

    try:
        validate_non_empty(tmp_path)
    except (FileNotFoundError, ValueError):
        logger.error(f"파일 검증 실패 : {filename}")
        raise

    # -----------------------------------------
    # 검증 완료
    # -----------------------------------------

    logger.info(
        f"검증 완료 : {filename}"
    )

    # 검증이 끝난 원본 dict를
    # 다음 Task인 store_bronze로 전달
    return download_result
