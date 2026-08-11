"""
Bronze 적재 모듈

역할
1. Bronze 폴더 저장
"""

import shutil

from airflow.decorators import task

from src.common.config import BRONZE_DIR
from src.common.logger import get_logger
from src.common.models import (
    DownloadResult,
    BronzeResult,
)

# Logger 생성
logger = get_logger(__name__)


@task
def store_bronze(
    downloaded_file: DownloadResult,
) -> BronzeResult:
    """Bronze 저장"""

    # Bronze 폴더 생성
    BRONZE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # 저장 경로
    bronze_path = BRONZE_DIR / downloaded_file.filename

    # 이미 저장된 파일
    if bronze_path.exists():

        logger.warning(
            f"이미 존재하는 파일입니다. 건너뜁니다 : {downloaded_file.filename}"
        )

        # 임시 파일 삭제
        if downloaded_file.tmp_path.exists():
            downloaded_file.tmp_path.unlink()

        return BronzeResult(
            taxi_type=downloaded_file.taxi_type,
            filename=downloaded_file.filename,
            bronze_path=bronze_path,
        )

    try:

        # Bronze 저장
        shutil.move(
            downloaded_file.tmp_path,
            bronze_path,
        )

        logger.info(
            f"Bronze 저장 완료 : {downloaded_file.filename}"
        )

        return BronzeResult(
            taxi_type=downloaded_file.taxi_type,
            filename=downloaded_file.filename,
            bronze_path=bronze_path,
        )

    except Exception as error:

        logger.error(
            f"Bronze 저장 실패 : {downloaded_file.filename} ({error})"
        )

        raise