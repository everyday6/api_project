"""
TLC ETL Pipeline
"""

from airflow.decorators import dag

from datetime import datetime

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


@dag(
    dag_id="tlc_pipeline",
    start_date=datetime(2025, 8, 1),
    schedule="@monthly",
    catchup=False,
    tags=["TLC"],
)
def tlc_pipeline():

    # 다운로드 목록 생성
    download_list = generate_download_list()

    # 파일 다운로드
    download_results = download_file.expand(
        file_info=download_list,
    )

    # 다운로드 검증
    validated_results = validate_download.expand(
        download_result=download_results,
    )

    # Bronze 저장
    bronze_results = store_bronze.expand(
        downloaded_file=validated_results,
    )

    # Silver 생성
    build_silver(
        bronze_results=bronze_results,
    )


tlc_pipeline()