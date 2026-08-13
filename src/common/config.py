"""
프로젝트 공통 설정 파일

프로젝트 전반에서 사용하는 설정값을 관리한다.
환경이 변경되더라도 이 파일만 수정하면 된다.
"""

import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

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
CONFIG_DIR = PROJECT_ROOT / "config"

TMP_DIR = DATA_DIR / "tmp"

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# ==========================
# HTTP 설정
# ==========================

# HEAD / GET Timeout
HTTP_TIMEOUT = 60

# 다운로드 Chunk 크기
CHUNK_SIZE = 8192

# User-Agent
USER_AGENT = {
    "User-Agent": "Traffic-Score-Project/1.0"
}

# ==========================
# NYC Open Data 설정
# ==========================

# 분석 대상 Borough
# 같은 맨해튼이지만 소스마다 표기가 달라 분리한다.
BOROUGH = "MANHATTAN"        # 공사 허가 (대문자)
BOROUGH_EVENT = "Manhattan"  # 행사 (첫 글자만 대문자)

# Socrata API 페이지 크기
SOCRATA_PAGE_SIZE = 50000

# NYC Open Data API URL
DATASETS = {
    "construction": "https://data.cityofnewyork.us/resource/tqtj-sjs8.json",
    "construction_stipulations": "https://data.cityofnewyork.us/resource/gsgx-6efw.json",
    "closure": "https://data.cityofnewyork.us/resource/ezy6-djsf.json",
    "event": "https://nycopendata.socrata.com/resource/tvpp-9vvx.json",
    "parks": "https://data.cityofnewyork.us/resource/enfh-gkve.json",
}

# ==========================
# Ticketmaster 설정
# ==========================

# Ticketmaster API Key (.env에서 불러옴)
TICKETMASTER_API_KEY = os.getenv("TICKETMASTER_API_KEY")

# Ticketmaster Discovery API URL
TICKETMASTER_URL = (
    "https://app.ticketmaster.com/discovery/v2/events.json"
)

# 조회 대상 도시
TICKETMASTER_CITY = "New York"

# API 페이지 크기
TICKETMASTER_PAGE_SIZE = 200

# API 최대 조회 가능 건수
# 이 값에 도달하면 초과분은 조용히 누락되므로
# CHUNK_DAYS 단위로 기간을 쪼개 호출한다.
TICKETMASTER_MAX_RESULTS = 1000

# API 호출 간 대기 시간
TICKETMASTER_SLEEP = 0.25

# 미래 이벤트 조회 기간
TICKETMASTER_LOOKAHEAD_DAYS = 120

TICKETMASTER_CHUNK_DAYS = 7

# ==========================
# Ticketmaster - LION 매핑 설정
# ==========================

# Ticketmaster 위경도 좌표계
TICKETMASTER_CRS = "EPSG:4326"

# NYC LION 좌표계
LION_CRS = "EPSG:2263"

# venue 주변 도로 매핑 반경 (feet)
TICKETMASTER_LION_BUFFER_FT = 200

# fallback nearest 매핑 품질 기준
TICKETMASTER_LION_WARN_DISTANCE_FT = 500
TICKETMASTER_LION_FAIL_DISTANCE_FT = 3000