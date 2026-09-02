"""
Bronze — 통행료 요금표/시설목록/CBD Geofence 폴리곤

세 가지 다 자동 수집이 아니라 사람이 관리하는 참조 데이터다(공식 요금
API가 없다 — docs/superpowers/specs/2026-08-21-navigation-gold-pipeline-design.md
참고). 이 파일은 로컬 config/*.yaml 파일과 CBD 폴리곤을 그대로
Bronze(S3 또는 로컬)에 올리는 역할만 한다 — 변환/파싱 없음(Bronze 원칙).

CBD Geofence는 data.ny.gov(Socrata)의 "MTA Central Business District
Geofence: Beginning June 2024" 데이터셋(srxy-5nxn)에서 받는다. 비슷한
이름의 vaq5-qfkz 데이터셋은 geometry가 비어있는 잘못된 데이터셋이라
혼동하지 말 것(curl로 실제 응답 내용 확인해서 검증함).
"""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from pathlib import Path

import requests

from src.common.config import BRONZE_DIR, TMP_DIR
from src.common.file_validation import validate_json, validate_non_empty, validate_yaml
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="toll_bronze")

SOURCE = "toll"
BRONZE_ROOT = BRONZE_DIR / SOURCE

CBD_GEOFENCE_URL = "https://data.ny.gov/resource/srxy-5nxn.geojson"


def _copy_file_to_bronze(source_path: str, out_path) -> None:
    """로컬 목적지는 파일 복사, S3 목적지는 객체 업로드로 저장한다.

    force_overwrite_to_cloud=True가 필요하다 - cloudpathlib은 로컬 파일
    mtime이 S3 객체의 최종 수정 시각보다 오래되면 "클라우드 쪽이 더
    최신인데 덮어쓰려 한다"고 보고 OverwriteNewerCloudError를 던진다.
    근데 git으로 체크아웃한 로컬 config/*.yaml은 내용이 안 바뀌면 mtime이
    안 갱신되는 반면 S3 객체의 최종 수정 시각은 매 업로드마다 갱신되므로,
    내용이 그대로여도 재실행할 때마다 이 에러가 났다(실제로 겪음). 여기는
    항상 로컬 파일(git 원본)이 진실 소스인 단방향 Bronze 적재라 클라우드
    쪽이 "더 최신"이라는 판단 자체가 의미가 없어 무조건 덮어쓴다."""

    source = Path(source_path)
    if isinstance(out_path, Path):
        shutil.copyfile(source, out_path)
    else:
        out_path.upload_from(source, force_overwrite_to_cloud=True)


def upload_rates(source_path: str = "config/toll_rates.yaml", bronze_root: Path = BRONZE_ROOT) -> Path:
    """toll_rates.yaml을 그대로 Bronze에 올린다."""

    validate_yaml(source_path)

    bronze_root.mkdir(parents=True, exist_ok=True)
    out_path = bronze_root / "toll_rates.yaml"
    _copy_file_to_bronze(source_path, out_path)

    logger.info(f"[toll_bronze] 요금표 업로드 완료 -> {out_path}")
    return out_path


def upload_facilities(source_path: str = "config/toll_facilities.yaml", bronze_root: Path = BRONZE_ROOT) -> Path:
    """toll_facilities.yaml을 그대로 Bronze에 올린다."""

    validate_yaml(source_path)

    bronze_root.mkdir(parents=True, exist_ok=True)
    out_path = bronze_root / "toll_facilities.yaml"
    _copy_file_to_bronze(source_path, out_path)

    logger.info(f"[toll_bronze] 시설목록 업로드 완료 -> {out_path}")
    return out_path


def _validate_cbd_geofence_content(path: Path) -> None:
    """형식(JSON) 검증은 file_validation.validate_json()에 맡기고, 여기서는
    "CBD Geofence로서 의미가 있는가"만 본다(도메인 검증이라 공통 모듈에
    안 둔다) - Socrata가 200을 주면서 몸통에 {"error": "..."} 같은 유효한
    JSON을 담아 보내는 경우까지는 validate_json()만으론 못 잡는다."""
    payload = json.loads(path.read_text())
    if payload.get("type") != "FeatureCollection":
        raise ValueError(f"CBD Geofence 응답이 FeatureCollection이 아닙니다: {path}")
    if not payload.get("features"):
        raise ValueError(f"CBD Geofence 응답의 features가 비어 있습니다: {path}")


# CBD Geofence는 URL(Socrata) 소스라 config/*.yaml처럼 git 이력이 없다.
# Bronze는 고정 경로에 in-place로 덮어쓰므로(파티셔닝은 후속 - 법적 경계라
# 변경이 거의 없음, RELIABILITY_PRINCIPLES.md Tier 2 #7 참고), 최소한
# "원본이 바뀌었는데 조용히 덮어쓰는" 상황은 감지한다: 내용 해시를 마커에
# 남겨, 같으면 Bronze 쓰기 자체를 건너뛰고 바뀌었으면 경고를 남긴다.
_CBD_GEOFENCE_HASH_MARKER = "_cbd_geofence_sha256.txt"


def _read_cbd_geofence_hash(bronze_root: Path) -> str | None:
    marker = bronze_root / _CBD_GEOFENCE_HASH_MARKER
    return marker.read_text().strip() if marker.exists() else None


def upload_cbd_geofence(url: str = CBD_GEOFENCE_URL, bronze_root: Path = BRONZE_ROOT) -> Path:
    """MTA CBD Geofence GeoJSON을 받아서 그대로 Bronze에 저장한다.

    로컬 tmp 파일에 먼저 받아서 검증하고, 통과해야만 Bronze(운영 경로)에
    반영한다 - 운영 경로에 먼저 쓰고 검증하면, 잘못된 응답이 이미 있던
    정상 파일을 덮어쓴 뒤에야 실패해서 그 순간부터 하위 파이프라인이
    깨진 파일을 그대로 쓰게 된다.

    내용 해시가 이전과 같으면 Bronze 쓰기를 건너뛴다(in-place 덮어쓰기라
    쓰는 것 자체가 무의미). 달라졌으면 이력 없이 덮어쓰는 것이므로 경고를
    남긴다 - 이 경고가 뜨면 lion/taxi_zone처럼 파티셔닝을 검토할 신호다."""

    bronze_root.mkdir(parents=True, exist_ok=True)
    out_path = bronze_root / "cbd_geofence.geojson"

    logger.info(f"[toll_bronze] CBD geofence 다운로드 시작: {url}")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    current_hash = hashlib.sha256(resp.content).hexdigest()
    previous_hash = _read_cbd_geofence_hash(bronze_root)

    if current_hash == previous_hash and out_path.exists():
        logger.info("[toll_bronze] CBD geofence 원본 변경 없음(해시 동일) — 저장 스킵")
        return out_path

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="toll_cbd_geofence_", dir=TMP_DIR) as tmp:
        tmp_path = Path(tmp) / "cbd_geofence.geojson"
        tmp_path.write_bytes(resp.content)

        # 다 받은 뒤에 실제로 유효한 JSON인지, CBD Geofence로서 의미가
        # 있는지 확인한다 - Socrata가 200을 주면서 에러 HTML/빈 응답을
        # 몸통에 담는 경우까지 잡기 위함. 여기서 실패하면 아직 운영
        # 경로(out_path)는 안 건드린 상태다.
        validate_non_empty(tmp_path)
        validate_json(tmp_path)
        _validate_cbd_geofence_content(tmp_path)

        if previous_hash is not None:
            logger.warning(
                "[toll_bronze] CBD geofence 원본 변경 감지(해시 %s -> %s) — "
                "이력 없이 in-place 덮어씀. 이전 경계로 만든 Silver2를 재구성해야 하면 "
                "파티셔닝 도입 필요(RELIABILITY_PRINCIPLES.md Tier 2 #7).",
                previous_hash[:12],
                current_hash[:12],
            )

        _copy_file_to_bronze(str(tmp_path), out_path)
        (bronze_root / _CBD_GEOFENCE_HASH_MARKER).write_text(current_hash)

    logger.info(f"[toll_bronze] CBD geofence 업로드 완료 -> {out_path}")
    return out_path


def main() -> None:
    upload_rates()
    upload_facilities()
    upload_cbd_geofence()


if __name__ == "__main__":
    main()
