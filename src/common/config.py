"""
프로젝트 공통 설정 파일

프로젝트 전반에서 사용하는 설정값을 관리한다.
환경이 변경되더라도 이 파일만 수정하면 된다.
"""

from datetime import datetime
from pathlib import Path

# ==========================
# TLC 데이터 설정
# ==========================

# TLC 원본 데이터 URL
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# 다운로드 대상 기간
# 최초 적재 시작 월
INITIAL_START_DATE = datetime(2022, 9, 1)
# 최초 적재 종료 월
INITIAL_END_DATE = datetime(2025, 8, 1)

# 다운로드할 택시 종류
TAXI_TYPES = [
    "yellow",
    "green",
    "fhv",
    "fhvhv",
]

# ==========================
# 운영 중 매일 신규 데이터 확인 설정
# ==========================

# TLC는 벤더 제출을 다 받으려고 보통 이 정도 지연을 두고 데이터를 올린다.
# 예: 8월이면 대략 (8월 - 2 =) 6월치까지 올라와 있음.
TLC_PUBLISH_LAG_MONTHS = 2

# 혹시 평소보다 더 늦게 올라온 달이 있을까봐,
# 기준 달(TLC_PUBLISH_LAG_MONTHS 전)부터 몇 달치를 더 여유 있게 확인할지
RECENT_MONTHS_WINDOW = 3

# ==========================
# 저장 경로
# ==========================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"

TMP_DIR = DATA_DIR / "tmp"

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"

# ==========================
# HTTP 설정
# ==========================

# HEAD / GET Timeout
HTTP_TIMEOUT = 30

# 다운로드 Chunk 크기
CHUNK_SIZE = 8192

# User-Agent
USER_AGENT = {
    "User-Agent": "Traffic-Score-Project/1.0"
}