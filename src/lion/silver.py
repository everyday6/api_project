"""
Silver 변환: LION bronze -> dim_segment

원본 코드 값(RW_TYPE, FeatureTyp, TrafDir 등)의 의미는 NYC DCP LION 공식
Data Dictionary를 직접 받아서 확인했다.
(https://www.nyc.gov/assets/planning/download/pdf/data-maps/open-data/lion_metadata.pdf)

스펙 대비 정정된 부분 (실제로 검증해서 뒤집힘):
- TrafDir: "W=양방향"으로 알려져 있었으나 공식 문서는 정반대다.
    W = With (일방향, 세그먼트 방향과 동일) / A = Against (일방향, 반대) / T = Two-Way (양방향)
  따라서 is_two_way = (TrafDir == 'T') 가 맞다.
- SegmentID 중복(전체의 약 10%)은 이중도로(SegCount>1, 상판/하판처럼 고도가 다른 도로)
  때문이 아니라, 실제로 뽑아서 확인해보니 geometry/속성/NodeLevel까지 완전히 동일한
  순수 중복 행이었다. 그래서 geometry를 합치지 않고 SegmentID 기준 첫 행만 남기는
  단순 dedupe를 적용한다.

road_class / capacity 관련 숫자는 팀에서 확정한 기준이 없어 HCM(Highway Capacity
Manual) 개념을 참고한 초안이다 — 반드시 검토/조정이 필요하다 (BASE_CAPACITY_PER_LANE 참고).

pandas를 쓰는 이유: LION은 분기 1회 갱신되는 24만 행짜리 참조 테이블이라 이 컴퓨터
한 대의 메모리로 몇 초면 끝난다. 처음엔 팀 컨벤션(tlc/silver.py)에 맞춰 PySpark로
짰었는데, 이 정도 규모에서는 득보다 실이 컸다(밑줄로 시작하는 파일을 숨김파일로 취급해
스키마 추론이 실패하는 문제, dedupe 한 번에 shuffle이 필요해지는 것, ANSI 모드에서
빈 문자열 캐스트가 예외를 던지는 것 등 — 전부 로컬 검증 과정에서 실제로 겪은 문제들).
데이터가 메모리에 안 들어갈 만큼 커지면 그때 Spark로 다시 바꾸는 게 맞다.

Spark든 pandas든 File Geodatabase(.gdb)를 직접 읽는 방법은 없어서, ogr2ogr(GDAL
CLI)로 필요한 컬럼 + WKT 지오메트리만 CSV로 평탄화한 뒤 그 CSV를 읽는다. 길이
(length_ft)는 LION이 이미 갖고 있는 SHAPE_Length 컬럼을 그대로 쓰고, geometry는
문자열(WKT)로만 보관한다 — 별도 공간연산(면적/교차 등)은 하지 않는다.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

from src.common.config import BRONZE_DIR, SILVER_DIR
from src.common.logger import get_logger
from src.common.utils import clean_street

logger = get_logger(__name__, log_to_file=True, log_file_stem="lion_silver")

LION_BRONZE_ROOT = BRONZE_DIR / "lion"
DIM_SEGMENT_PATH = SILVER_DIR / "dim_segment.parquet"

# ogr2ogr로 뽑아올 컬럼. LION 원본 필드 그대로의 이름을 쓴다.
LION_COLUMNS = [
    "SegmentID", "Street", "RW_TYPE", "TRUCK_ROUTE_TYPE", "TrafDir",
    "FeatureTyp", "Number_Travel_Lanes", "Number_Total_Lanes",
    "StreetWidth_Min", "StreetWidth_Max", "SHAPE_Length", "LBoro",
]

# RW_TYPE(도로유형 코드, 공식 정의) -> road_class 1차 분류
HIGHWAY_RW_TYPES = ["2", "9"]                              # Highway, Ramp
NON_ROUTABLE_RW_TYPES = ["5", "6", "7", "8", "10", "11", "12", "13", "14"]
# Boardwalk, Path/Trail, Step Street, Driveway, Alley, Unknown, Non-Physical Segment, U-Turn, Ferry Route
# RW_TYPE 1(Street)/3(Bridge)/4(Tunnel)은 등급 구분이 없어서 TRUCK_ROUTE_TYPE(1=Limited
# Local, 2=Local, 3=Through)과 차로수로 arterial/local을 보조 판정한다.
ARTERIAL_TRUCK_ROUTE_TYPES = ["2", "3"]
ARTERIAL_MIN_LANES = 3

# TODO(팀 검토 필요): 확정된 사내 기준이 없어 HCM(도로용량편람) 개념 기반 초안.
# 단위: 차로당 시간당 승용차환산대수(pcphpl)
BASE_CAPACITY_PER_LANE = {
    "highway": 1900,   # HCM 자유류(freeway) 이상적 포화교통류율 근사치
    "arterial": 900,   # HCM 신호교차로 도시간선도로 차로당 용량 근사치
    "local": 600,       # 저속/주차회전 마찰이 있는 국지도로 근사치
    "non_routable": 0,
}

# TODO(팀 검토 필요): 방향계수 확정 기준 없어 1.0(보정 없음)으로 둠.
DIRECTION_FACTOR = {
    "one_way": 1.0,
    "two_way": 1.0,
}


def _latest_bronze_version(bronze_root: Path = LION_BRONZE_ROOT) -> Path:
    """bronze/lion/version_date=YYYY-MM-DD 파티션 중 가장 최신 것을 찾는다."""
    versions = sorted(bronze_root.glob("version_date=*"))
    if not versions:
        raise FileNotFoundError(f"LION bronze 데이터가 없습니다: {bronze_root}")
    return versions[-1]


def _find_gdb(version_dir: Path) -> Path:
    gdbs = list(version_dir.rglob("*.gdb"))
    if not gdbs:
        raise FileNotFoundError(f"{version_dir} 안에 .gdb가 없습니다")
    return gdbs[0]


def _gdb_to_flat_csv(gdb_path: Path, out_path: Path) -> Path:
    """
    ogr2ogr로 LION 'lion' 레이어(File Geodatabase)에서 필요한 컬럼 + geometry(WKT)만
    뽑아 평면 CSV로 변환한다. Docker 이미지에 gdal-bin(ogr2ogr)이 설치돼 있어야 한다.

    컬럼 선택은 "-sql SELECT ... FROM lion" 대신 "-select"를 쓴다 — -sql 커스텀 쿼리는
    GDAL 버전에 따라 geometry 컬럼(SHAPE)을 결과에서 빠뜨리는 경우가 있었다(로컬 Mac의
    최신 GDAL에서는 문제없었지만 Docker 이미지의 apt gdal-bin에서 실제로 재현됨).
    -select는 속성 필드를 고르면서 geometry는 항상 유지하도록 설계된 옵션이라 더 안전하다.

    "-nlt CONVERT_TO_LINEAR": LION 세그먼트 중 일부(전체의 약 3.7%)는 곡선 도로라서
    원본 geometry가 CIRCULARSTRING/COMPOUNDCURVE 같은 비선형 WKT로 나온다. shapely 등
    일반적인 geometry 라이브러리는 이 타입을 못 읽어서(map_zone_segment 매핑 작업 중
    실제로 겪음), 직선 근사(LINESTRING)로 강제 변환한다.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "ogr2ogr",
        "-f", "CSV",
        str(out_path),
        str(gdb_path),
        "lion",
        "-select", ",".join(LION_COLUMNS),
        "-lco", "GEOMETRY=AS_WKT",
        "-nlt", "CONVERT_TO_LINEAR",
    ]
    logger.info(f"[lion_silver] ogr2ogr 실행: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"[lion_silver] ogr2ogr 실패: {result.stderr}")
        raise RuntimeError(f"ogr2ogr 변환 실패: {result.stderr}")

    return out_path


def _classify_road_class(df: pd.DataFrame) -> pd.Series:
    """RW_TYPE(+TRUCK_ROUTE_TYPE, 차로수 보조)로 road_class를 매긴다."""
    is_highway = df["RW_TYPE"].isin(HIGHWAY_RW_TYPES)
    is_non_routable = df["RW_TYPE"].isin(NON_ROUTABLE_RW_TYPES) | df["RW_TYPE"].isna() | (df["RW_TYPE"] == "")
    is_arterial = (
        df["TRUCK_ROUTE_TYPE"].isin(ARTERIAL_TRUCK_ROUTE_TYPES)
        | (df["Number_Travel_Lanes"] >= ARTERIAL_MIN_LANES)
    )

    return pd.Series(
        np.select(
            [is_highway, is_non_routable, is_arterial],
            ["highway", "non_routable", "arterial"],
            default="local",
        ),
        index=df.index,
    )


def build_dim_segment(
    bronze_root: Path = LION_BRONZE_ROOT,
    silver_root: Path = SILVER_DIR,
) -> str:
    """LION 최신 bronze 스냅샷을 읽어 dim_segment Silver 테이블을 만든다."""

    version_dir = _latest_bronze_version(bronze_root)
    gdb_path = _find_gdb(version_dir)
    logger.info(f"[lion_silver] 입력 bronze: {gdb_path}")

    # 주의: 파일명이 "_"로 시작하면 Hadoop/Spark 계열 도구가 숨김 파일(_SUCCESS 등과
    # 동일 취급)로 보고 무시하는 경우가 있어(직접 겪은 문제) 밑줄로 시작하지 않게 짓는다.
    tmp_csv = silver_root / "lion_flat_tmp.csv"
    _gdb_to_flat_csv(gdb_path, tmp_csv)

    df = pd.read_csv(tmp_csv, dtype=str, keep_default_na=False)

    # ogr2ogr -lco GEOMETRY=AS_WKT로 만든 geometry 컬럼 이름이 GDAL 버전마다 다르다
    # (직접 확인: 로컬 GDAL 3.13은 "SHAPE", Docker 이미지의 GDAL 3.6.2는 "WKT").
    # 버전에 의존하지 않도록 둘 다 찾아서 "SHAPE"로 통일한다.
    if "SHAPE" not in df.columns:
        if "WKT" in df.columns:
            df = df.rename(columns={"WKT": "SHAPE"})
        else:
            raise RuntimeError(
                f"geometry 컬럼(SHAPE/WKT)을 찾을 수 없습니다. 실제 컬럼: {list(df.columns)}"
            )

    # 숫자 컬럼 정제 — 원본이 전부 문자열이고 앞 공백/빈 문자열이 섞여있다(실 데이터로 확인).
    df["RW_TYPE"] = df["RW_TYPE"].str.strip()
    df["TRUCK_ROUTE_TYPE"] = df["TRUCK_ROUTE_TYPE"].str.strip()
    df["Number_Travel_Lanes"] = pd.to_numeric(df["Number_Travel_Lanes"].str.strip(), errors="coerce")
    df["SHAPE_Length"] = pd.to_numeric(df["SHAPE_Length"], errors="coerce")

    # SegmentID 중복(약 10%) 제거 — 실제로는 geometry/속성까지 완전히 동일한 순수 중복이라
    # 병합 없이 첫 행만 남긴다.
    before = len(df)
    df = df.drop_duplicates(subset="SegmentID", keep="first")
    logger.info(f"[lion_silver] dedupe: {before}행 -> {len(df)}행")

    df["road_class"] = _classify_road_class(df)
    df["is_routable"] = (df["road_class"] != "non_routable") & (df["FeatureTyp"] == "0")
    df["is_two_way"] = df["TrafDir"] == "T"

    df["base_capacity_per_lane"] = df["road_class"].map(BASE_CAPACITY_PER_LANE)
    direction_factor = np.where(df["is_two_way"], DIRECTION_FACTOR["two_way"], DIRECTION_FACTOR["one_way"])
    df["capacity_per_hour"] = df["Number_Travel_Lanes"] * df["base_capacity_per_lane"] * direction_factor
    df["lane_miles"] = (df["SHAPE_Length"] * df["Number_Travel_Lanes"]) / 5280.0

    # construction/road_closures 등 다른 소스와 동일한 규칙으로 정규화 — 도로명은
    # 공유하지만 교차로(from/to street)는 LION 세그먼트 자체엔 없어서, 이 값만으로는
    # "어느 도로인지"까지만 좁혀지고 "어느 블록인지"는 아직 못 좁힌다.
    df["street_name"] = df["Street"].map(clean_street)

    dim_segment = df.rename(
        columns={
            "SegmentID": "segment_id",
            "LBoro": "borough_code",
            "SHAPE": "geometry",
            "SHAPE_Length": "length_ft",
            "Number_Travel_Lanes": "lanes_total",
        }
    )[[
        "segment_id", "borough_code", "geometry", "length_ft", "road_class",
        "is_two_way", "lanes_total", "lane_miles", "base_capacity_per_lane",
        "capacity_per_hour", "is_routable", "street_name",
    ]]

    dim_segment_path = silver_root / "dim_segment.parquet"

    silver_root.mkdir(parents=True, exist_ok=True)
    dim_segment.to_parquet(dim_segment_path, index=False)

    logger.info(f"[lion_silver] dim_segment {len(dim_segment)}행 저장 -> {dim_segment_path}")

    tmp_csv.unlink(missing_ok=True)
    return str(dim_segment_path)


VALID_ROAD_CLASSES = ["highway", "arterial", "local", "non_routable"]
VALID_BOROUGH_CODES = ["1", "2", "3", "4", "5"]
# 빈 값도 허용한다 — 경계선/비물리적 구간처럼 자치구가 없는 세그먼트가 실제로 존재한다
# (직접 확인: 전체의 0.56%, 그중 대부분은 non_routable이지만 routable인데 빈 경우도 있음).

# 지금까지 실제로 확인된 행 수(218,373)를 기준으로 여유 있게 잡은 범위.
# 분기 갱신마다 이 범위를 크게 벗어나면 ogr2ogr/파싱 단계가 조용히 깨졌을 가능성이 크다.
MIN_EXPECTED_ROWS = 100_000
MAX_EXPECTED_ROWS = 300_000


def validate_dim_segment(path: str) -> str:
    """
    dim_segment.parquet가 지켜야 할 최소한의 불변식을 확인한다.
    하나라도 깨지면 AssertionError를 던져서 태스크를 실패시킨다 — 조용히 잘못된
    데이터가 다음 단계로 넘어가는 걸 막는 게 목적이다.
    """
    df = pd.read_parquet(path)

    assert df["segment_id"].is_unique, "segment_id 중복 발견 (dedupe 로직 확인 필요)"

    routable_missing_geom = df.loc[df["is_routable"], "geometry"].isna()
    assert not routable_missing_geom.any(), (
        f"is_routable=True인데 geometry가 없는 행 {routable_missing_geom.sum()}개 발견"
    )

    assert df["road_class"].isin(VALID_ROAD_CLASSES).all(), (
        f"알 수 없는 road_class 값: {sorted(set(df['road_class']) - set(VALID_ROAD_CLASSES))}"
    )

    assert df["borough_code"].isin(VALID_BOROUGH_CODES + [""]).all(), (
        f"알 수 없는 borough_code 값: {sorted(set(df['borough_code']) - set(VALID_BOROUGH_CODES) - {''})}"
    )

    n = len(df)
    assert MIN_EXPECTED_ROWS <= n <= MAX_EXPECTED_ROWS, (
        f"행 수가 예상 범위({MIN_EXPECTED_ROWS}~{MAX_EXPECTED_ROWS}) 밖입니다: {n}"
    )

    logger.info(f"[lion_silver] dim_segment 검증 통과 ({n}행) -> {path}")
    return path


if __name__ == "__main__":
    out = build_dim_segment()
    validate_dim_segment(out)
