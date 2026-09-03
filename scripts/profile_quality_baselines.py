"""공개 원천/로컬 산출물에서 품질 게이트 baseline을 재현해 측정한다.

이 스크립트는 운영 파이프라인을 실행하거나 산출물을 publish하지 않는다.
현재 LION FileGDB와 선택적으로 Taxi Zone Shapefile을 읽어, 코드에 정의된
``is_suspect`` 조건과 중복 충돌/공간 매핑 분포만 JSON으로 출력한다.

Examples
--------
APP_ENV=local python scripts/profile_quality_baselines.py lion \
  --gdb data/bronze/lion/version_date=2026-09-03/lion/lion.gdb \
  --output data/profiles/lion-2026-09-03.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import pyogrio

from src.lion.expectations import SPEED_LIMIT_MAX_MPH, SPEED_LIMIT_MIN_MPH
from src.lion.silver1 import _profile_duplicates


LION_PROFILE_COLUMNS = [
    "SegmentID",
    "Street",
    "RW_TYPE",
    "TRUCK_ROUTE_TYPE",
    "TrafDir",
    "FeatureTyp",
    "Number_Travel_Lanes",
    "SHAPE_Length",
    "LBoro",
    "NodeIDFrom",
    "NodeIDTo",
    "POSTED_SPEED",
]


def _json_number(value: float) -> float | None:
    """JSON에 NaN/Infinity를 쓰지 않도록 유한한 값만 반환한다."""

    return float(value) if pd.notna(value) else None


def _describe_percent(values: pd.Series) -> dict[str, float | None]:
    if values.empty:
        return {key: None for key in ("min", "p50", "p95", "p99", "max")}
    quantiles = values.quantile([0.5, 0.95, 0.99])
    return {
        "min": _json_number(values.min()),
        "p50": _json_number(quantiles.loc[0.5]),
        "p95": _json_number(quantiles.loc[0.95]),
        "p99": _json_number(quantiles.loc[0.99]),
        "max": _json_number(values.max()),
    }


def _load_lion(gdb_path: Path) -> pd.DataFrame:
    frame = pyogrio.read_dataframe(
        gdb_path,
        layer="lion",
        columns=LION_PROFILE_COLUMNS,
    )
    geometry_name = frame.geometry.name
    raw = pd.DataFrame(frame.drop(columns=[geometry_name]))
    # 운영 ogr2ogr 경로의 ``GEOMETRY=AS_WKT`` 출력과 같은 비교 가능한 형태.
    raw["SHAPE"] = frame.geometry.to_wkt()
    return raw


def profile_lion(gdb_path: Path) -> tuple[dict, pd.DataFrame]:
    raw = _load_lion(gdb_path)
    duplicate_profile = _profile_duplicates(raw)

    # 운영 코드와 같은 핵심 필드 순서로 결정적 dedup을 수행한다. pyogrio가
    # 숫자 필드를 숫자로 읽는 차이만 있을 뿐 판정 의미는 동일하다.
    conflict_columns = ["SHAPE", "SHAPE_Length", "NodeIDFrom", "NodeIDTo", "LBoro"]
    deduped = (
        raw.sort_values(["SegmentID", *conflict_columns], kind="stable")
        .drop_duplicates(subset="SegmentID", keep="first")
        .copy()
    )

    length_ft = pd.to_numeric(deduped["SHAPE_Length"], errors="coerce")
    lanes_total = pd.to_numeric(deduped["Number_Travel_Lanes"], errors="coerce")
    speed_limit = pd.to_numeric(deduped["POSTED_SPEED"], errors="coerce")
    node_from_missing = deduped["NodeIDFrom"].isna() | (
        deduped["NodeIDFrom"].astype(str).str.strip() == ""
    )
    node_to_missing = deduped["NodeIDTo"].isna() | (
        deduped["NodeIDTo"].astype(str).str.strip() == ""
    )

    reasons = pd.DataFrame(
        {
            "negative_length": length_ft.notna() & (length_ft < 0),
            "negative_lanes": lanes_total.notna() & (lanes_total < 0),
            "speed_limit_out_of_range": speed_limit.notna()
            & (
                (speed_limit < SPEED_LIMIT_MIN_MPH)
                | (speed_limit > SPEED_LIMIT_MAX_MPH)
            ),
            "node_from_missing": node_from_missing,
            "node_to_missing": node_to_missing,
        },
        index=deduped.index,
    )
    suspect = reasons.any(axis=1)
    row_count = len(deduped)

    dim_segment = pd.DataFrame(
        {
            "segment_id": deduped["SegmentID"].astype(str),
            "geometry": deduped["SHAPE"],
        }
    )

    result = {
        "source": str(gdb_path),
        "raw": duplicate_profile,
        "deduped_rows": row_count,
        "suspect_rows": int(suspect.sum()),
        "suspect_ratio": float(suspect.mean()) if row_count else 0.0,
        "suspect_reasons": {
            name: {
                "rows": int(mask.sum()),
                "ratio": float(mask.mean()) if row_count else 0.0,
            }
            for name, mask in reasons.items()
        },
        "reference_distributions": {
            "speed_limit_missing": {
                "rows": int(speed_limit.isna().sum()),
                "ratio": float(speed_limit.isna().mean()) if row_count else 0.0,
            },
            "speed_limit_mph": _describe_percent(speed_limit.dropna()),
            "length_ft": _describe_percent(length_ft.dropna()),
            "lanes_total": _describe_percent(lanes_total.dropna()),
        },
    }
    return result, dim_segment


def _write_result(result: dict, output: Path | None) -> None:
    rendered = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False)
    print(rendered)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    lion_parser = subparsers.add_parser("lion")
    lion_parser.add_argument("--gdb", type=Path, required=True)
    lion_parser.add_argument("--output", type=Path)
    lion_parser.add_argument(
        "--dim-segment-output",
        type=Path,
        help="silver2 프로파일링에 사용할 최소 segment_id/geometry Parquet",
    )

    args = parser.parse_args()
    if args.command == "lion":
        result, dim_segment = profile_lion(args.gdb)
        if args.dim_segment_output is not None:
            args.dim_segment_output.parent.mkdir(parents=True, exist_ok=True)
            dim_segment.to_parquet(args.dim_segment_output, index=False)
        _write_result(result, args.output)


if __name__ == "__main__":
    main()
