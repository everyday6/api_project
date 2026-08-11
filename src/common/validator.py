"""
공통 파일 검증 모듈

역할:
1. 다운로드 파일 존재 여부 확인
2. 다운로드 파일 크기 확인

Airflow Task 간 데이터 전달은
커스텀 객체가 아닌 dict를 사용한다.
"""

from airflow.decorators import task

from src.common.logger import get_logger


# Logger 생성
logger = get_logger(__name__)


@task
def validate_download(
    download_result: dict,
) -> dict:
    """
    다운로드된 파일을 검증한다.

    입력:
        {
            "taxi_type": "...",
            "filename": "...",
            "tmp_path": "..."
        }

    반환:
        검증에 성공한 동일한 dict
    """

    # -------------------------------------------------
    # 파일 정보 추출
    # -------------------------------------------------

    filename = download_result["filename"]

    # downloader.py에서 tmp_path를 문자열로 전달했으므로
    # 파일 시스템에서 사용할 수 있도록 Path 객체로 변환
    from pathlib import Path

    tmp_path = Path(
        download_result["tmp_path"]
    )

    # -------------------------------------------------
    # 1. 파일 존재 여부 확인
    # -------------------------------------------------

    if not tmp_path.exists():

        logger.error(
            f"파일이 존재하지 않습니다 : {filename}"
        )

        raise FileNotFoundError(
            tmp_path
        )

    # -------------------------------------------------
    # 2. 파일 크기 확인
    # -------------------------------------------------

    if tmp_path.stat().st_size == 0:

        logger.error(
            f"빈 파일입니다 : {filename}"
        )

        raise ValueError(
            f"빈 파일입니다 : {filename}"
        )

    # -------------------------------------------------
    # 3. 검증 성공
    # -------------------------------------------------

    logger.info(
        f"검증 완료 : {filename}"
    )

    # 객체로 변환하지 않고
    # 기존 dict를 그대로 다음 Task로 전달
    return download_result