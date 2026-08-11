"""
공통 검증 모듈

역할
1. 다운로드 파일 존재 여부 확인
2. 다운로드 파일 크기 확인
"""

from airflow.decorators import task

from src.common.logger import get_logger
from src.common.models import DownloadResult

# Logger 생성
logger = get_logger(__name__)


@task
def validate_download(
    download_result: DownloadResult,
) -> DownloadResult:
    """다운로드 파일 검증"""

    # 파일 존재 여부 확인
    if not download_result.tmp_path.exists():

        logger.error(
            f"파일이 존재하지 않습니다 : {download_result.filename}"
        )

        raise FileNotFoundError(
            download_result.tmp_path
        )

    # 파일 크기 확인
    if download_result.tmp_path.stat().st_size == 0:

        logger.error(
            f"빈 파일입니다 : {download_result.filename}"
        )

        raise ValueError(
            f"빈 파일입니다 : {download_result.filename}"
        )

    logger.info(
        f"검증 완료 : {download_result.filename}"
    )

    return download_result