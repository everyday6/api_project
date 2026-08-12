"""
TLC 전체 ETL Pipeline

파일 하나를 기준으로:

Download
    ↓
Validate
    ↓
Bronze
    ↓
Silver

의 전체 파이프라인을 구성하고,
이 파이프라인을 TLC 파일 개수만큼 실행한다.

각 파일의 파이프라인은 서로 독립적으로 처리된다.
"""

from datetime import datetime

from airflow.decorators import dag, task_group

from src.common.downloader import (
    generate_download_list,
    download_file,
)

from src.common.validator import (
    validate_download,
)

from src.tlc.bronze import (
    store_bronze,
)

from src.tlc.silver import (
    build_silver,
)


# =========================================================
# 파일 하나 처리
# =========================================================

@task_group
def process_file(file_info: dict):
    """
    TLC 파일 하나의 전체 ETL 과정을 처리한다.

    Download
        ↓
    Validate
        ↓
    Bronze
        ↓
    Silver
    """

    # -----------------------------------------
    # 1. 다운로드
    # -----------------------------------------

    downloaded = download_file(
        file_info=file_info,
    )

    # -----------------------------------------
    # 2. 다운로드 파일 검증
    # -----------------------------------------

    validated = validate_download(
        download_result=downloaded,
    )

    # -----------------------------------------
    # 3. Bronze 저장
    # -----------------------------------------

    bronze = store_bronze(
        downloaded_file=validated,
    )

    # -----------------------------------------
    # 4. Silver 생성
    # -----------------------------------------

    build_silver(
        bronze_result=bronze,
    )


# =========================================================
# DAG
# =========================================================

@dag(
    dag_id="tlc_pipeline",
    start_date=datetime(2025, 8, 1),
    schedule="@monthly",
    catchup=False,
    tags=["TLC"],
)
def tlc_pipeline():

    # -----------------------------------------
    # 다운로드 대상 파일 목록 생성
    # -----------------------------------------

    download_list = generate_download_list()

    # -----------------------------------------
    # 파일별 전체 파이프라인 실행
    #
    # 파일 하나마다:
    #
    # Download
    #   ↓
    # Validate
    #   ↓
    # Bronze
    #   ↓
    # Silver
    #
    # 의 독립적인 파이프라인이 생성된다.
    # -----------------------------------------

    process_file.expand(
        file_info=download_list,
    )


# DAG 생성
tlc_pipeline()