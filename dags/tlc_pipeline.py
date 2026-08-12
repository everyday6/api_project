"""
TLC 전체 ETL Pipeline

Download
    ↓
Validate
    ↓
Bronze
    ↓
Silver

의 전체 파이프라인을 단계별로 구성한다.

파일별로 독립적으로 흘러가는 방식이 아니라,
한 단계(예: Download)가 전체 파일에 대해 다 끝나야
다음 단계(예: Validate)가 전체적으로 시작된다.
"""

from datetime import datetime

from airflow.decorators import dag

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
    # 1. 다운로드 대상 파일 목록 생성
    # -----------------------------------------

    download_list = generate_download_list()

    # -----------------------------------------
    # 2. 전체 다운로드
    # -----------------------------------------

    downloaded_files = download_file.expand(
        file_info=download_list,
    )

    # -----------------------------------------
    # 3. 전체 검증
    # -----------------------------------------

    validated_files = validate_download.expand(
        download_result=downloaded_files,
    )

    # -----------------------------------------
    # 4. 전체 Bronze 저장
    # -----------------------------------------

    bronze_files = store_bronze.expand(
        downloaded_file=validated_files,
    )

    # -----------------------------------------
    # 5. 전체 Silver 변환
    # -----------------------------------------

    build_silver.expand(
        bronze_result=bronze_files,
    )


# DAG 생성
tlc_pipeline()
