"""
Bronze ingestion: NYC DCP LION (Single Line Street Base Map)

주의: LION은 Socrata에서 "non-tabular"(지도 전용) 자산으로 등록되어 있어서
$limit/$offset 같은 행 단위 API 조회가 불가능하다 (실제로 시도하면
"no row or column access to non-tabular tables" 에러가 남).

그래서 taxi_zone의 shapefile과 동일한 방식으로, NYC DCP가 제공하는
파일(zip) 원본을 통째로 받아서 그대로 압축 해제한다.
분기마다 새 버전이 나오는 전체 스냅샷 데이터라, 증분 개념 없이
매번 전체를 받고 version_date로만 구분한다.

정상 스케줄(분기 1회)대로면 매번 실제로 새 버전이지만, 재시도/수동
재실행으로 같은 분기에 두 번 돌면 원본이 그대로인데도 전체 다운로드
+ Silver1 재계산 + downstream Asset(lion_bronze_updated,
lion_dim_segment_ready) 재트리거까지 낭비된다. 그래서 taxi_zone과
동일한 ETag 비교로 원본이 그대로면 스킵한다(src/taxi_zone/bronze.py
참고). 단, LION_ZIP_URL은 Socrata 리다이렉트(302)를 거쳐야 실제
ETag가 있는 리소스에 도달하므로 allow_redirects=True가 필수다 -
안 그러면 requests.head()가 리다이렉트 응답만 보고 ETag 없이 끝난다
(실제로 curl/requests로 재현해서 확인함).
"""

from __future__ import annotations

import io
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

from src.common.config import BRONZE_DIR, TMP_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="lion")

# NYC Open Data 데이터셋 페이지(data.cityofnewyork.us/City-Government/LION/2v4z-66xt)에서
# 직접 확인한 실제 다운로드 링크.
LION_ZIP_URL = "https://data.cityofnewyork.us/download/2v4z-66xt/application%2Fzip"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

BRONZE_ROOT = BRONZE_DIR / "lion"


def _read_previous_etag(bronze_root: Path) -> str | None:
    """직전 성공 시점의 ETag를 읽는다.

    version_date별로 매번 새 디렉터리(version_date=...)가 생기는 LION은
    taxi_zone처럼 고정된 산출물 경로에 _metadata.txt를 두고 거기서 읽을
    수 없다 - bronze_root 바로 아래에 별도의 고정 마커 파일을 둔다.
    파일이 없거나(첫 실행) _etag 줄이 없으면 None을 반환하고, 호출부는
    이를 "무조건 새로 받아야 함"으로 취급한다."""

    marker_path = bronze_root / "_latest_etag.txt"
    if not marker_path.exists():
        return None

    for line in marker_path.read_text().splitlines():
        if line.startswith("_etag="):
            return line.split("=", 1)[1].strip()
    return None


def _write_latest_etag(bronze_root: Path, etag: str) -> None:
    (bronze_root / "_latest_etag.txt").write_text(
        f"_etag={etag}\n_updated_at={datetime.now(timezone.utc).isoformat()}\n"
    )


def _contains_gdb_file(root) -> bool:
    """root 아래에 실제 내용이 들어 있는 File Geodatabase가 있는지 확인한다."""

    return any(
        path.is_file() and ".gdb/" in str(path).replace("\\", "/").lower()
        for path in root.rglob("*")
    )


def _upload_tree(local_root: Path, destination) -> None:
    """로컬 디렉터리의 파일을 로컬/S3 목적지에 재귀적으로 복사한다."""

    if isinstance(destination, Path):
        shutil.copytree(local_root, destination, dirs_exist_ok=True)
        return

    destination.mkdir(parents=True, exist_ok=True)
    for local_path in local_root.rglob("*"):
        if not local_path.is_file():
            continue
        relative_path = local_path.relative_to(local_root)
        (destination / relative_path.as_posix()).upload_from(local_path)


def ingest_lion(version_date: str | None = None, bronze_root=BRONZE_ROOT) -> dict:
    """
    version_date: 'YYYY-MM-DD' 형식. 안 주면 오늘 날짜로 자동 태깅.
    (Airflow에서는 '{{ ds }}'를 그대로 넘기면 됨)

    반환값의 "changed"는 build_dim_segment_staged가 원본이 그대로일 때
    재계산 자체를 건너뛰도록(AirflowSkipException) 판단하는 데 쓰인다.

    이 함수는 ETag 마커(_latest_etag.txt)를 직접 쓰지 않는다 - 그건
    mark_lion_etag()가 Silver1 publish까지 전부 성공한 뒤에만 한다(DAG
    맨 끝). 여기서 다운로드 직후에 바로 써버리면, 그 뒤 Silver1
    (validate/publish)이 실패해도 마커는 이미 새 버전을 가리키게 되어
    다음 스케줄 실행이 "원본 그대로"로 보고 재시도 자체를 영원히
    건너뛰는 사고가 난다 - 주 1회 정기 확인 사이에 실패한 run을 사람이
    수동으로 clear하지 않더라도 다음 주간 실행이 다시 복구를 시도해야 한다.
    대신 반환값에 etag를 실어서, 호출부(DAG)가 파이프라인 끝에서
    넘겨준다."""

    if version_date is None:
        version_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    head_response = requests.head(LION_ZIP_URL, headers=HEADERS, timeout=30, allow_redirects=True)
    head_response.raise_for_status()
    current_etag = head_response.headers.get("ETag", "").strip('"')

    previous_etag = _read_previous_etag(bronze_root)
    if current_etag and current_etag == previous_etag:
        logger.info(
            "[lion] 원본 변경 없음(ETag %s 동일) — version_date=%s 다운로드 스킵",
            current_etag,
            version_date,
        )
        return {"path": None, "changed": False, "etag": current_etag}

    if previous_etag:
        logger.info(
            "[lion] 원본 변경 감지(ETag %s -> %s) — version_date=%s 다운로드",
            previous_etag,
            current_etag,
            version_date,
        )

    logger.info(f"[lion] version_date={version_date} 다운로드 시작: {LION_ZIP_URL}")

    try:
        resp = requests.get(LION_ZIP_URL, headers=HEADERS, timeout=180)
        resp.raise_for_status()
    except requests.RequestException:
        logger.exception(f"[lion] version_date={version_date} 다운로드 실패: {LION_ZIP_URL}")
        raise

    # zipfile은 S3Path에 직접 압축을 풀 수 없다. 로컬 스크래치 공간에 먼저
    # 압축을 푼 다음 완성된 파일만 Bronze(S3)에 올린다.
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="lion_bronze_", dir=TMP_DIR) as tmp:
        extract_dir = Path(tmp) / "extracted"
        extract_dir.mkdir()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            z.extractall(extract_dir)

        if not _contains_gdb_file(extract_dir):
            raise RuntimeError("LION ZIP 안에 유효한 .gdb 파일이 없습니다")

        dest_dir = bronze_root / f"version_date={version_date}"
        _upload_tree(extract_dir, dest_dir)

        # 업로드가 일부만 됐는데 성공 로그가 찍히는 false success를 막는다.
        if not _contains_gdb_file(dest_dir):
            raise RuntimeError(f"LION .gdb S3 업로드 검증 실패: {dest_dir}")

        marker_path = dest_dir / "_metadata.txt"
        marker_path.write_text(
            f"_ingested_at={datetime.now(timezone.utc).isoformat()}\n"
            f"_source=nyc_dcp_lion\n"
            f"_source_url={LION_ZIP_URL}\n"
            f"_etag={current_etag}\n"
        )

        if not marker_path.exists():
            raise RuntimeError(f"LION 메타데이터 업로드 검증 실패: {marker_path}")

    logger.info(f"[lion] version_date={version_date} 압축 해제 완료 -> {dest_dir}")
    return {"path": str(dest_dir), "changed": True, "etag": current_etag}


def mark_lion_etag(bronze_version_result: dict, bronze_root=BRONZE_ROOT) -> None:
    """ETag 마커(_latest_etag.txt)를 이제야 갱신한다 - Airflow DAG에서
    Silver1 publish_dim_segment까지 성공했을 때만(그 태스크 뒤에 이어서)
    호출돼야 한다. ingest_lion() 자체가 이 마커를 쓰지 않는 이유는 그
    함수 docstring 참고.

    changed=False(원본 그대로라 이번 실행이 처음부터 스킵된 경우)면
    build_dim_segment_staged가 AirflowSkipException을 던져서 이 태스크까지
    기본 trigger_rule(all_success)로 자동 스킵된다 - 마커가 이미 맞는
    값을 가리키고 있으니 다시 쓸 필요가 없어 별문제 없다."""
    etag = bronze_version_result.get("etag")
    if etag:
        _write_latest_etag(bronze_root, etag)


if __name__ == "__main__":
    ingest_lion()
