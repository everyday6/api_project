"""TLC 원본 수집 → S3 Gold2(Type 3 Zone 일별 집계) 생성 파이프라인.

TLC 데이터는 정확히 한 달 뒤가 아니라 몇 달씩 지연을 두고
불규칙하게 올라오기 때문에, 특정 날짜를 기다리는 대신
매일 다음 공개 후보 1개월과 최근 완료 3개월을 확인한다.
(이미 Bronze에 있는 파일은 건너뜀)

첫 실행도 같은 상대 기간을 사용해 초기 데이터를 채우며, 이후 실행에서는
새로 공개된 월별 파일만 처리한다. 별도의 과거 전체 초기 적재 DAG는 없다.

Download
    ↓
Validate
    ↓
Bronze
    ↓
Validate (Great Expectations, EMR Serverless)
    ↓
Silver1 (EMR Serverless)
    ↓
Zone 날짜별 Type 3 (EMR Serverless → S3 Gold2)

S3 Gold2를 RDS(서빙 DB)에 발행하는 건 별도 DAG(tlc_type3_serving_daily)가
맡는다 - "데이터 생성"과 "서비스 DB 발행"의 역할을 분리해서, RDS 쪽에
문제가 생겨도 이 DAG(다운로드~Silver~Gold2)를 다시 돌릴 필요가 없게
한다. 두 DAG는 강하게 연결하지 않고 각자 독립적으로 매일 실행된다.

신규 파일이 없는 날은 각 단계가 빈 목록에 대해 실행되어
아무 일도 하지 않고 정상 종료된다.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag

from src.common.alerts import notify_slack_failure
from src.common.downloader import (
    generate_incremental_download_list,
    download_file,
)

from src.common.validator import (
    validate_download,
)

from src.tlc.bronze import (
    store_bronze,
)

from src.tlc.bronze_validation import (
    chunk_bronze_files,
    validate_bronze_quality,
)
from src.tlc.silver1 import build_silver
from src.tlc.type3_pipeline import (
    build_type3_staged_records,
    cleanup_type3_staging,
    publish_type3_daily_records,
    validate_type3_staged_records,
)

# download_file.expand()가 파일 개수만큼 태스크 인스턴스를 만드는 mapped
# task라서, default_args에 on_failure_callback을 넣으면 실패한 인덱스마다
# Slack이 따로 온다(예: 120개 중 20개 실패 -> 알림 20개). retries만 여기
# 걸고, on_failure_callback은 아래 @dag()에 DAG 레벨로 따로 걸어서 이 DAG
# run 전체가 실패로 확정될 때 딱 한 번만 오게 한다.
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# =========================================================
# DAG
# =========================================================

@dag(
    dag_id="tlc_ingest_daily",
    start_date=datetime(2025, 8, 1),
    schedule="@daily",
    catchup=False,
    # 이전 실행이 아직 안 끝났는데 다음 실행이 겹쳐서 시작되면
    # 같은 파일(tmp 경로가 run별로 안 나뉘어 있음)을 두고 충돌할 수 있어서
    # 동시에 1개만 실행되게 제한
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=notify_slack_failure,
    tags=["TLC", "ingest", "daily"],
)
def tlc_ingest_daily():

    # -----------------------------------------
    # 1. 신규 데이터 확인
    # -----------------------------------------

    download_list = generate_incremental_download_list()

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
    # 5. taxi_type별 청크로 묶기
    # -----------------------------------------

    bronze_chunks = chunk_bronze_files(
        bronze_files=bronze_files,
    )

    # -----------------------------------------
    # 6. 청크별 Bronze 데이터 품질 검증 (Great Expectations)
    # -----------------------------------------

    validated_bronze_chunks = validate_bronze_quality.expand(
        bronze_chunk=bronze_chunks,
    )

    # -----------------------------------------
    # 7. 검증 통과 파일 Silver1 변환 및 S3 저장
    # -----------------------------------------

    silver_results = build_silver.expand(
        bronze_chunk=validated_bronze_chunks,
    )

    # -----------------------------------------
    # 8. Type 3 임시 저장 → 검증 → 운영 파티션(S3 Gold2) 승격
    #    (오늘 신규 Silver 기준)
    # -----------------------------------------

    staged_type3 = build_type3_staged_records(silver_results)
    validated_type3 = validate_type3_staged_records(staged_type3)
    published_type3 = publish_type3_daily_records(validated_type3)
    cleanup_type3_staging(published_type3)


# DAG 생성
tlc_ingest_daily()
