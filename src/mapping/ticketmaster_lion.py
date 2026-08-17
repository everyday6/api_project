"""
Ticketmaster Silver -> LION segment mapping

역할
- Ticketmaster venue 좌표를 LION segment에 매핑
- Ticketmaster EPSG:4326 -> LION EPSG:2263 좌표 변환
- 차량 통행 가능한 LION segment만 사용
- venue 기준 200ft 이내의 모든 segment 매핑
- 200ft 안에 segment가 없는 venue는 nearest segment 1개 fallback
- mapping 결과를 run_date 기준 Parquet으로 저장
"""

import os
import time
from datetime import date
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely import wkt

from src.common.config import (
    SILVER_DIR,
    TICKETMASTER_CRS,
    LION_CRS,
    TICKETMASTER_LION_BUFFER_FT,
    TICKETMASTER_LION_WARN_DISTANCE_FT,
    TICKETMASTER_LION_FAIL_DISTANCE_FT,
)
from src.common.logger import get_logger
from src.common.utils import save_parquet


logger = get_logger(__name__, log_to_file=True, log_file_stem="map_ticketmaster_lion")

SOURCE = "ticketmaster"


# =========================================================
# 경로
# =========================================================

def ticketmaster_path(run_date: str) -> Path:
    return (
        SILVER_DIR
        / SOURCE
        / f"dt={run_date}"
        / "data.parquet"
    )


def lion_path() -> Path:
    return (
        SILVER_DIR
        / "dim_segment.parquet"
    )


def output_dir(run_date: str) -> Path:
    return (
        SILVER_DIR
        / "mapping"
        / "ticketmaster_lion"
        / f"dt={run_date}"
    )


# =========================================================
# Load
# =========================================================

def load_ticketmaster(
    run_date: str,
) -> pd.DataFrame:

    path = ticketmaster_path(run_date)

    if not path.exists():
        raise FileNotFoundError(
            f"Ticketmaster Silver 파일 없음: {path}"
        )

    logger.info(
        "Ticketmaster Silver 로드 시작: path=%s",
        path,
    )

    df = pd.read_parquet(path)

    logger.info(
        "Ticketmaster Silver 로드 완료: rows=%d columns=%d",
        len(df),
        len(df.columns),
    )

    return df


def load_lion() -> pd.DataFrame:

    path = lion_path()

    if not path.exists():
        raise FileNotFoundError(
            f"LION dim_segment 파일 없음: {path}"
        )

    logger.info(
        "LION 로드 시작: path=%s",
        path,
    )

    df = pd.read_parquet(path)

    logger.info(
        "LION 로드 완료: rows=%d columns=%d",
        len(df),
        len(df.columns),
    )

    return df


# =========================================================
# Input validation
# =========================================================

def validate_ticketmaster_input(
    df: pd.DataFrame,
):

    required_cols = [
        "event_id",
        "event_date",
        "start_ts",
        "end_ts",
        "venue_name",
        "lat",
        "lon",
    ]

    missing = [
        col
        for col in required_cols
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Ticketmaster 필수 컬럼 없음: {missing}"
        )

    if df.empty:
        raise ValueError(
            "Ticketmaster Silver가 비었습니다."
        )

    if df["event_id"].isna().any():
        raise ValueError(
            "Ticketmaster event_id NULL 발생"
        )

    if not df["event_id"].is_unique:
        dup = int(
            df["event_id"].duplicated().sum()
        )

        raise ValueError(
            f"Ticketmaster event_id 중복 발생: {dup}건"
        )

    invalid_coord = int(
        (
            df["lat"].isna()
            | df["lon"].isna()
        ).sum()
    )

    logger.info(
        "Ticketmaster 입력 검증 완료: "
        "rows=%d invalid_coord=%d",
        len(df),
        invalid_coord,
    )


def validate_lion_input(
    df: pd.DataFrame,
):

    required_cols = [
        "segment_id",
        "geometry",
        "is_routable",
    ]

    missing = [
        col
        for col in required_cols
        if col not in df.columns
    ]

    if missing:
        raise ValueError(
            f"LION 필수 컬럼 없음: {missing}"
        )

    if df.empty:
        raise ValueError(
            "LION dim_segment가 비었습니다."
        )

    if not df["segment_id"].is_unique:
        dup = int(
            df["segment_id"].duplicated().sum()
        )

        raise ValueError(
            f"LION segment_id 중복 발생: {dup}건"
        )

    logger.info(
        "LION 입력 검증 완료: rows=%d routable=%d",
        len(df),
        int(df["is_routable"].sum()),
    )


# =========================================================
# GeoDataFrame 생성
# =========================================================

def build_ticketmaster_gdf(
    df: pd.DataFrame,
) -> gpd.GeoDataFrame:

    before = len(df)

    work = df[
        df["lat"].notna()
        & df["lon"].notna()
    ].copy()

    if work.empty:
        raise ValueError(
            "Ticketmaster 유효 좌표가 없습니다."
        )

    logger.info(
        "Ticketmaster 좌표 필터: %d -> %d",
        before,
        len(work),
    )

    gdf = gpd.GeoDataFrame(
        work,
        geometry=gpd.points_from_xy(
            work["lon"],
            work["lat"],
        ),
        crs=TICKETMASTER_CRS,
    )

    return gdf.to_crs(
        LION_CRS
    )


def build_lion_gdf(
    df: pd.DataFrame,
) -> gpd.GeoDataFrame:

    before = len(df)

    work = df[
        df["is_routable"]
        & df["geometry"].notna()
    ].copy()

    if work.empty:
        raise ValueError(
            "사용 가능한 LION routable segment가 없습니다."
        )

    logger.info(
        "LION routable 필터: %d -> %d",
        before,
        len(work),
    )

    try:
        work["geometry"] = (
            work["geometry"]
            .apply(wkt.loads)
        )

    except Exception as e:
        raise ValueError(
            "LION WKT geometry 파싱 실패"
        ) from e

    return gpd.GeoDataFrame(
        work,
        geometry="geometry",
        crs=LION_CRS,
    )


# =========================================================
# Mapping
# =========================================================

def map_ticketmaster_to_lion(
    ticketmaster_df: pd.DataFrame,
    lion_df: pd.DataFrame,
) -> pd.DataFrame:

    tm_gdf = build_ticketmaster_gdf(
        ticketmaster_df
    )

    lion_gdf = build_lion_gdf(
        lion_df
    )

    logger.info(
        "Ticketmaster-LION buffer mapping 시작: "
        "events=%d lion=%d buffer=%dft",
        len(tm_gdf),
        len(lion_gdf),
        TICKETMASTER_LION_BUFFER_FT,
    )

    started = time.perf_counter()

    # -----------------------------------------------------
    # 1. venue Point 주변에 200ft buffer 생성
    # -----------------------------------------------------

    buffer_gdf = tm_gdf.copy()

    buffer_gdf["geometry"] = (
        buffer_gdf.geometry.buffer(
            TICKETMASTER_LION_BUFFER_FT
        )
    )

    # -----------------------------------------------------
    # 2. buffer와 겹치는 모든 LION segment 찾기
    # -----------------------------------------------------

    joined = gpd.sjoin(
        buffer_gdf,
        lion_gdf[
            [
                "segment_id",
                "geometry",
            ]
        ],
        how="left",
        predicate="intersects",
    )

    # -----------------------------------------------------
    # 3. 실제 venue Point ↔ segment 거리 계산
    #
    # buffer에 들어왔다는 사실만으로는 정확한 거리를
    # 알 수 없으므로 원래 venue Point를 이용한다.
    # -----------------------------------------------------

    lion_geometry = (
        lion_gdf
        .set_index("segment_id")
        .geometry
    )

    def calculate_distance(row):

        segment_id = row["segment_id"]

        if pd.isna(segment_id):
            return pd.NA

        point = tm_gdf.loc[
            row.name,
            "geometry",
        ]

        segment = lion_geometry.loc[
            segment_id
        ]

        return point.distance(
            segment
        )

    joined["distance_ft"] = (
        joined.apply(
            calculate_distance,
            axis=1,
        )
    )

    # -----------------------------------------------------
    # 4. 200ft 안에 segment가 하나도 없는 이벤트 확인
    # -----------------------------------------------------

    mapped_event_ids = set(
        joined.loc[
            joined["segment_id"].notna(),
            "event_id",
        ]
    )

    fallback_gdf = tm_gdf[
        ~tm_gdf["event_id"].isin(
            mapped_event_ids
        )
    ].copy()

    logger.info(
        "buffer 내 segment 없는 이벤트: %d",
        len(fallback_gdf),
    )

    # -----------------------------------------------------
    # 5. 없는 이벤트만 nearest segment fallback
    # -----------------------------------------------------

    if not fallback_gdf.empty:

        nearest = gpd.sjoin_nearest(
            fallback_gdf,
            lion_gdf[
                [
                    "segment_id",
                    "geometry",
                ]
            ],
            how="left",
            distance_col="distance_ft",
        )

        nearest["mapping_method"] = (
            "nearest_fallback"
        )

    else:

        nearest = None

    # -----------------------------------------------------
    # 6. buffer 매핑 결과
    # -----------------------------------------------------

    buffer_result = joined[
        joined["segment_id"].notna()
    ].copy()

    buffer_result[
        "mapping_method"
    ] = "buffer"

    # -----------------------------------------------------
    # 7. buffer + fallback 합치기
    # -----------------------------------------------------

    result_columns = [
        "event_id",
        "event_date",
        "start_ts",
        "end_ts",
        "venue_name",
        "lat",
        "lon",
        "segment_id",
        "distance_ft",
        "mapping_method",
    ]

    results = [
        buffer_result[
            result_columns
        ]
    ]

    if nearest is not None:
        results.append(
            nearest[
                result_columns
            ]
        )

    result = pd.concat(
        results,
        ignore_index=True,
    )

    # 동일 event-segment 중복 방어
    result = result.drop_duplicates(
        subset=[
            "event_id",
            "segment_id",
        ],
        keep="first",
    )

    elapsed = (
        time.perf_counter()
        - started
    )

    logger.info(
        "Ticketmaster-LION mapping 완료: "
        "rows=%d elapsed=%.2fs",
        len(result),
        elapsed,
    )

    return result


# =========================================================
# Output validation
# =========================================================

def _validate_result(
    df: pd.DataFrame,
    input_df: pd.DataFrame,
):

    if df.empty:
        raise ValueError(
            "Ticketmaster-LION 매핑 결과가 비었습니다."
        )

    # event_id + segment_id가 PK 역할
    duplicated = df.duplicated(
        subset=[
            "event_id",
            "segment_id",
        ]
    )

    if duplicated.any():
        raise ValueError(
            "event_id + segment_id 중복 발생: "
            f"{int(duplicated.sum())}건"
        )

    # 입력 중 좌표가 존재했던 이벤트
    valid_input_events = set(
        input_df.loc[
            input_df["lat"].notna()
            & input_df["lon"].notna(),
            "event_id",
        ]
    )

    mapped_events = set(
        df["event_id"]
    )

    missing_events = (
        valid_input_events
        - mapped_events
    )

    if missing_events:
        raise ValueError(
            "좌표가 있지만 LION 매핑에 실패한 이벤트: "
            f"{len(missing_events)}건"
        )

    buffer_rows = int(
        (
            df["mapping_method"]
            == "buffer"
        ).sum()
    )

    fallback_rows = int(
        (
            df["mapping_method"]
            == "nearest_fallback"
        ).sum()
    )

    unique_events = (
        df["event_id"]
        .nunique()
    )

    avg_segments = (
        len(df)
        / unique_events
    )

    max_segments = (
        df.groupby("event_id")
        ["segment_id"]
        .nunique()
        .max()
    )

    logger.info(
        "매핑 검증 완료: "
        "events=%d mapping_rows=%d "
        "avg_segments=%.2f max_segments=%d "
        "buffer_rows=%d fallback_rows=%d",
        unique_events,
        len(df),
        avg_segments,
        max_segments,
        buffer_rows,
        fallback_rows,
    )

    # fallback 거리 품질 검사
    fallback = df[
        df["mapping_method"]
        == "nearest_fallback"
    ]

    if not fallback.empty:

        over_warn = int(
            (
                fallback["distance_ft"]
                > TICKETMASTER_LION_WARN_DISTANCE_FT
            ).sum()
        )

        over_fail = int(
            (
                fallback["distance_ft"]
                > TICKETMASTER_LION_FAIL_DISTANCE_FT
            ).sum()
        )

        logger.info(
            "fallback 거리 품질: "
            "count=%d >%dft=%d >%dft=%d",
            len(fallback),
            TICKETMASTER_LION_WARN_DISTANCE_FT,
            over_warn,
            TICKETMASTER_LION_FAIL_DISTANCE_FT,
            over_fail,
        )

        if over_warn:
            logger.warning(
                "fallback 중 %dft 초과: %d건",
                TICKETMASTER_LION_WARN_DISTANCE_FT,
                over_warn,
            )

        if over_fail:
            raise ValueError(
                "fallback 매핑 거리가 "
                f"{TICKETMASTER_LION_FAIL_DISTANCE_FT}ft를 "
                f"초과한 이벤트 {over_fail}건 발생"
            )


# =========================================================
# Save
# =========================================================

def save_mapping(
    df: pd.DataFrame,
    run_date: str,
) -> str:

    out_dir = output_dir(
        run_date
    )

    logger.info(
        "매핑 결과 저장 시작: path=%s",
        out_dir,
    )

    path = save_parquet(
        df,
        out_dir,
    )

    logger.info(
        "매핑 결과 저장 완료: rows=%d path=%s",
        len(df),
        path,
    )

    return str(path)


# =========================================================
# Pipeline
# =========================================================

def build_ticketmaster_lion_mapping(
    run_date: str,
) -> str:

    started = time.perf_counter()

    logger.info(
        "Ticketmaster-LION 파이프라인 시작: run_date=%s",
        run_date,
    )

    ticketmaster_df = (
        load_ticketmaster(
            run_date
        )
    )

    lion_df = load_lion()

    validate_ticketmaster_input(
        ticketmaster_df
    )

    validate_lion_input(
        lion_df
    )

    result = map_ticketmaster_to_lion(
        ticketmaster_df,
        lion_df,
    )

    path = save_mapping(
        result,
        run_date,
    )

    elapsed = (
        time.perf_counter()
        - started
    )

    logger.info(
        "Ticketmaster-LION 파이프라인 빌드 완료: "
        "run_date=%s rows=%d elapsed=%.2fs path=%s",
        run_date,
        len(result),
        elapsed,
        path,
    )

    return path


def validate_output(path: str, run_date: str) -> str:
    """build_ticketmaster_lion_mapping()이 저장한 결과를 다시 읽어, 그 run_date의
    원본 ticketmaster 입력과 비교하며 _validate_result()를 돌린다."""
    df = pd.read_parquet(path)
    input_df = load_ticketmaster(run_date)
    _validate_result(df, input_df)
    return path


def main(
    run_date: str | None = None,
) -> str:

    if run_date is None:
        run_date = os.getenv(
            "RUN_DATE",
            date.today().isoformat(),
        )

    path = build_ticketmaster_lion_mapping(
        run_date
    )
    validate_output(path, run_date)
    return path


if __name__ == "__main__":
    main()