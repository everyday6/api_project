"""공통 파일 형식 검증 — 도메인을 몰라도 되는 순수 포맷 체크만 담당한다.

Bronze에 올리기 직전에 "이 파일이 애초에 열리는가"를 확인하는 용도다.
데이터 의미(taxi_type별 필수 컬럼, 값 범위 등)는 각 도메인의 GX
Expectation(src/tlc/expectations.py, src/speed/expectations.py 등)이
맡는다 — 여긴 형식만 본다(LION/Taxi Zone의 zip 검증, TLC/Speed의 GX
검증처럼 이미 각 도메인에 흩어져 있던 "파일이 파싱되는가" 체크를 한
곳으로 모은 것).

전부 검증 실패 시 예외를 던진다(호출부가 알아서 처리) — 파일이 이미
Bronze에 올라가기 전에 여기서 막는 게 목적이라, 여기서 조용히 삼키면
안 된다.

검증 대상은 항상 다운로드 직후의 로컬 tmp 파일이다(Bronze 업로드 전
단계) — S3Path를 다룰 필요가 없다.
"""

from __future__ import annotations

import json
import zipfile
from fnmatch import fnmatch
from pathlib import Path

import pyarrow.parquet as pq
import yaml


def validate_non_empty(path: Path | str) -> None:
    """파일이 존재하고(디렉터리 아님) 0바이트가 아닌지 확인한다."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"파일이 존재하지 않습니다: {path}")
    if not path.is_file():
        raise ValueError(f"파일이 아닙니다(디렉터리 등): {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"빈 파일입니다: {path}")


def validate_parquet(path: Path | str, *, required_columns: list[str] | None = None) -> None:
    """Parquet으로 실제로 열리는지 확인한다.

    ParquetFile로 메타데이터/스키마만 읽는다(전체 로드 안 함) — TLC
    월별 파일처럼 큰 파일도 가볍게 확인할 수 있다. required_columns를
    주면 그 컬럼들이 스키마에 있는지도 같이 확인하지만, 값 자체의
    정상성(범위 등)은 검증하지 않는다 — 그건 GX Expectation의 몫이다.
    """
    try:
        schema = pq.ParquetFile(str(path)).schema_arrow
    except Exception as exc:
        raise ValueError(f"유효한 Parquet 파일이 아닙니다: {path} ({exc})") from exc

    if required_columns:
        missing = set(required_columns) - set(schema.names)
        if missing:
            raise ValueError(f"Parquet에 필수 컬럼이 없습니다: {path} (누락: {sorted(missing)})")


def validate_zip(
    path: Path | str,
    *,
    required_files: list[str] | None = None,
    deep_check: bool = False,
) -> None:
    """ZIP으로 실제로 열리는지 확인한다.

    required_files를 주면 (fnmatch 기준) 그 패턴에 맞는 항목이 최소
    하나씩 있는지도 확인한다 — 예: LION은 ["*.gdb/*"]로 GDB 디렉터리가
    실제 내용을 담고 있는지 본다.

    deep_check=True면 testzip()으로 내부 파일 전체의 CRC까지 확인한다 -
    ZIP 전체를 한 번 다 읽으므로 LION처럼 큰 ZIP엔 기본으로 켜두면 곧이어
    할 압축 해제와 합쳐 파일을 두 번 읽는 셈이 된다. 기본값은 꺼둔다."""
    try:
        with zipfile.ZipFile(str(path)) as zf:
            names = zf.namelist()
            first_bad_file = zf.testzip() if deep_check else None
    except zipfile.BadZipFile as exc:
        raise ValueError(f"유효한 ZIP 파일이 아닙니다: {path} ({exc})") from exc

    if first_bad_file is not None:
        raise ValueError(f"ZIP 안 파일이 손상됐습니다: {path} (손상된 항목: {first_bad_file})")

    if required_files:
        for pattern in required_files:
            if not any(fnmatch(name, pattern) for name in names):
                raise ValueError(
                    f"ZIP 안에 필요한 파일이 없습니다: {path} (패턴: {pattern})"
                )


def validate_yaml(path: Path | str) -> None:
    """YAML로 실제로 파싱되고, 빈 문서가 아닌지 확인한다.

    yaml.safe_load("")는 예외가 아니라 None을 반환하므로(빈 문서도
    문법적으로는 유효한 YAML), validate_non_empty()로 빈 파일 자체를
    먼저 걸러낸다."""
    validate_non_empty(path)
    try:
        yaml.safe_load(Path(path).read_text())
    except yaml.YAMLError as exc:
        raise ValueError(f"유효한 YAML 파일이 아닙니다: {path} ({exc})") from exc


def validate_json(path: Path | str) -> None:
    """JSON으로 실제로 파싱되는지 확인한다(GeoJSON도 JSON의 부분집합이라
    별도 파서 없이 이걸로 충분하다)."""
    try:
        json.loads(Path(path).read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"유효한 JSON 파일이 아닙니다: {path} ({exc})") from exc
