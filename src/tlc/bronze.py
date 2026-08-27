"""
Bronze 적재 모듈

역할
1. 다운로드된 파일이 실제로 열리는 Parquet인지 확인
2. 다운로드된 파일을 Bronze 폴더에 저장
3. 다음 Task에서 사용할 Bronze 파일 정보를 dict로 반환

Airflow Task 간 데이터 전달은 dict를 사용한다.

validate_download(src/common/validator.py)가 "파일이 존재하고 비어있지
않은지"는 이미 확인했지만, "이게 진짜 Parquet으로 열리는지"는 아직
아니다 - LION(GDB 존재 확인)/Taxi Zone(shp 존재 확인)은 압축을 풀어보는
것 자체가 이 확인을 겸하는데, TLC는 다운로드한 파일을 그대로 업로드만
해서 이 검증이 빠져있었다. taxi_type별 컬럼 값 검증(GX)은 별도
downstream 단계(src/tlc/bronze_validation.py)의 몫이라 여기서는 형식만
본다.
"""

from pathlib import Path

from airflow.decorators import task

from src.common.config import BRONZE_DIR
from src.common.file_validation import validate_parquet
from src.common.logger import get_logger


BRONZE_ROOT = BRONZE_DIR / "tlc"

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

    BRONZE_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    # -----------------------------------------
    # Bronze 저장 경로
    # -----------------------------------------

    bronze_path = BRONZE_ROOT / filename

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

        # 실제로 열리는 Parquet인지 먼저 확인한다 - 깨진 파일이 Bronze에
        # 올라가면 Silver 단계의 Spark job이 읽다가 죽을 때까지 아무도
        # 모른다.
        validate_parquet(tmp_path)

        # bronze_path는 S3Path — 로컬 tmp 파일을 업로드하고, 성공하면
        # 로컬 tmp는 지운다(shutil.move는 로컬 전용이라 S3엔 못 씀).
        bronze_path.upload_from(tmp_path)
        tmp_path.unlink()

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