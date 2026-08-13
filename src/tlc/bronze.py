"""
Bronze 적재 모듈

역할
1. 다운로드된 파일을 Bronze 폴더에 저장
2. 다음 Task에서 사용할 Bronze 파일 정보를 dict로 반환

Airflow Task 간 데이터 전달은 dict를 사용한다.
"""

import shutil
from pathlib import Path

from airflow.decorators import task

from src.common.config import BRONZE_DIR
from src.common.logger import get_logger


# Logger 생성
logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_bronze")


@task
def store_bronze(
    downloaded_file: dict,
) -> dict:
    """다운로드된 파일 하나를 Bronze에 저장한다."""

    # -----------------------------------------
    # 다운로드 결과에서 필요한 정보 추출
    # -----------------------------------------

    taxi_type = downloaded_file["taxi_type"]
    filename = downloaded_file["filename"]

    # XCom으로 전달된 경로는 문자열이므로
    # 실제 Path 객체로 변환
    tmp_path = Path(
        downloaded_file["tmp_path"]
    )

    # -----------------------------------------
    # Bronze 폴더 생성
    # -----------------------------------------

    BRONZE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # -----------------------------------------
    # Bronze 저장 경로
    # -----------------------------------------

    bronze_path = BRONZE_DIR / filename

    # -----------------------------------------
    # 이미 Bronze에 존재하는 경우
    # -----------------------------------------

    if bronze_path.exists():

        logger.warning(
            f"이미 존재하는 파일입니다. 건너뜁니다 : "
            f"{filename}"
        )

        # 임시 파일 삭제
        if tmp_path.exists():
            tmp_path.unlink()

        return {
            "taxi_type": taxi_type,
            "filename": filename,
            "bronze_path": str(bronze_path),
        }

    # -----------------------------------------
    # Bronze 저장
    # -----------------------------------------

    try:

        shutil.move(
            tmp_path,
            bronze_path,
        )

        logger.info(
            f"Bronze 저장 완료 : {filename}"
        )

        # -----------------------------------------
        # 다음 Task로 dict 전달
        # -----------------------------------------

        return {
            "taxi_type": taxi_type,
            "filename": filename,
            "bronze_path": str(bronze_path),
        }

    except Exception as error:

        logger.error(
            f"Bronze 저장 실패 : "
            f"{filename} ({error})"
        )

        raise