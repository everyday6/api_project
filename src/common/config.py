"""
프로젝트 공통 설정 파일

프로젝트 전반에서 사용하는 설정값을 관리한다.
환경이 변경되더라도 이 파일만 수정하면 된다.
"""

import os
from datetime import datetime
from pathlib import Path

from cloudpathlib import S3Path
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

CONFIG_DIR = PROJECT_ROOT / "config"
LOG_DIR = PROJECT_ROOT / "logs"

# 다운로드 스트리밍 중간 저장용 로컬 스크래치 공간. S3는 부분 업로드를
# 노출하지 않아 스트리밍 write가 안 되므로, 로컬에 받았다가 완료 후
# S3로 올린다(예: src/tlc/bronze.py의 store_bronze).
TMP_DIR = PROJECT_ROOT / "data" / "tmp"

# S3 버킷 이름 (.env에서 불러옴). 로컬/EC2 밖에서는 SCP가 이 버킷 접근
# 자체를 막아서 실제로는 EC2 인스턴스의 IAM 롤에서만 동작한다.
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_DATA = os.getenv("S3_BUCKET_DATA")
S3_BUCKET_DASHBOARD = os.getenv("S3_BUCKET_DASHBOARD")

# Bronze/Silver/Gold는 S3Path — cloudpathlib가 pathlib과 동일한 인터페이스
# (glob/mkdir/exists/unlink/`/` 등)를 제공해서 기존 호출부 대부분은 그대로
# 동작한다. 다만 pandas I/O(read_parquet/to_parquet 등)에 넘길 때는
# str(path)로 변환해야 한다(안 그러면 pandas가 로컬 캐시 경로로 오해한다).
BRONZE_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/bronze")

SILVER1_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/silver1")
SILVER2_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/silver2")
GOLD1_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/gold1")
GOLD2_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/gold2")

# ==========================
# RDS (Gold 서빙 테이블) 설정
# ==========================

RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = os.getenv("RDS_PORT", "5432")
RDS_DB = os.getenv("RDS_DB")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")

# ==========================
# HTTP 설정
# ==========================

# HEAD / GET Timeout
HTTP_TIMEOUT = 60

# 한 페이지 요청 전체(내부 재연결 포함)에 거는 하드 데드라인. requests의
# timeout은 소켓 read 호출 "한 번"에만 걸려서, 서버가 응답을 아주 느리게
# 찔끔찔끔 흘려보내면(각 read는 HTTP_TIMEOUT 안쪽이라 안 걸림) 전체 요청은
# 시간제한 없이 계속 매달릴 수 있다(실제로 겪음 — 첫 페이지 요청이 1시간
# 넘게 안 끊기고 매달려 있다가 서버 쪽에서 강제로 RemoteDisconnected로 끊음).
# src/common/socrata.py의 _get_page가 이 값으로 별도 스레드에 하드 데드라인을
# 건다.
SOCRATA_PAGE_HARD_TIMEOUT = 90

# 다운로드 Chunk 크기
CHUNK_SIZE = 8192

# User-Agent
USER_AGENT = {
    "User-Agent": "Traffic-Score-Project/1.0"
}

# ==========================
# 알림 설정
# ==========================

# Slack Incoming Webhook URL (.env에서 불러옴)
# 여기서 없다고 바로 에러내지 않는다 — config.py는 거의 모든 DAG가 공통으로
# 임포트하는 파일이라, 여기서 raise하면 알림과 무관한 파이프라인까지 전부
# 깨진다. 실제로 알림을 보내는 시점(src/common/alerts.py)에서만 확인한다.
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

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
# 여기서 없다고 바로 에러내지 않는다 — config.py는 거의 모든 DAG가 공통으로
# 임포트하는 파일이라, 여기서 raise하면 Ticketmaster와 무관한 파이프라인까지
# 전부 깨진다. 실제로 이 키가 필요한 시점(src/ticketmaster/bronze.py의
# build())에서만 검증한다.
TICKETMASTER_API_KEY = os.getenv("TICKETMASTER_API_KEY")

# Gemini API Key (.env에서 불러옴) — construction_stipulations의 WORK EMBARGO
# 정규식 파싱 실패건을 LLM으로 한 번 더 시도할 때 씀(src/common/gemini.py).
# 위와 동일한 이유로 여기서 없다고 바로 에러내지 않는다.
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

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

# ==========================
# 2016 하차 위경도 Hotspot 설정
# ==========================

# BigQuery로 받은 2016년 하차 위경도 grid(bq-results.csv)의 좌표계.
# TLC가 2017년부터 정확한 위경도 대신 zone_id만 제공하므로, 위경도 기준으로
# zone 내부 분포를 볼 수 있는 마지막 해 데이터다.
BQ_HOTSPOT_CRS = "EPSG:4326"

# zone 내부 세그먼트별 spatial_weight 계산 시, grid point가 0건 매칭된
# 세그먼트도 완전히 0이 되지 않게 하는 라플라스 스무딩 상수. 정성적 초안이다
# (TODO, 팀 검토 필요) — docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md 참고.
LAPLACE_SMOOTHING_ALPHA = 1.0

# grid point 하나(8~11m 셀)가 세그먼트에 매칭될 때, 이 반경(feet) 이내 세그먼트
# 전부를 후보로 삼아 거리 역가중으로 나눠 배분한다. venue-도로 매핑에 쓴
# TICKETMASTER_LION_BUFFER_FT(200ft)보다 좁게 잡은 이유는 grid 셀 자체가 훨씬
# 작기 때문이다. 정성적 초안이다(TODO, 팀 검토 필요).
HOTSPOT_SEGMENT_BUFFER_FT = 100

# 반경 안 세그먼트에 거리 역가중(1/(distance+epsilon))을 매길 때, point가 세그먼트
# 위에 정확히 있어 distance=0이 되는 경우의 0-division만 막는 최소 상수. 정성적
# 초안이다(TODO, 팀 검토 필요).
HOTSPOT_INVERSE_DISTANCE_EPSILON_FT = 1.0