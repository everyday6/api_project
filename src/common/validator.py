"""
공통 검증 모듈

역할
1. 다운로드 파일 존재 여부 확인
2. 다운로드 파일 크기 확인
3. 검증된 다운로드 정보를 dict 형태로 다음 Task에 전달
"""

from pathlib import Path

from airflow.decorators import task

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
    # 1. 파일 존재 여부 확인
    # -----------------------------------------

    if not tmp_path.exists():

        logger.error(
            f"파일이 존재하지 않습니다 : {filename}"
        )

        raise FileNotFoundError(
            tmp_path
        )

    # -----------------------------------------
    # 2. 파일 크기 확인
    # -----------------------------------------

    if tmp_path.stat().st_size == 0:

        logger.error(
            f"빈 파일입니다 : {filename}"
        )

        raise ValueError(
            f"빈 파일입니다 : {filename}"
        )

    # -----------------------------------------
    # 검증 완료
    # -----------------------------------------

    logger.info(
        f"검증 완료 : {filename}"
    )

    # 검증이 끝난 원본 dict를
    # 다음 Task인 store_bronze로 전달
    return download_result