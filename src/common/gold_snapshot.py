"""
Gold 서빙 데이터의 "마지막으로 성공한 값" 스냅샷.

RDS(src/common/rds.py)가 완전히 응답 불가능할 때 쓰는 폴백
(src/serving/nav_lookup.py 참고) - S3는 이미 멀티 AZ로 복제되는 관리형
스토리지라 RDS(Multi-AZ 안 쓰면 단일 인스턴스)보다 죽기 어렵다.

세그먼트당 AVG/SPEC/가장 최근 exact 값만 담는다(하루치 버킷 이력 전부는
안 담음 - 스냅샷을 쓰는 시점엔 오래된 실측값도 어차피 freshness 기준을
넘겨 못 쓰므로 최신 1개면 충분하고, 그만큼 스냅샷 크기가 작아진다).

Gold 파이프라인이 RDS에 쓰기 성공할 때마다 RDS의 현재 상태를 그대로
다시 내보내는 방식이다(부분 병합이 아니라 매번 전체 재수출) - 여러
파이프라인(30분 주기 실시간 버킷, 분기 SPEC)이 같은 스냅샷 파일에 부분
병합을 시도하면 경합으로 값이 유실될 수 있는데, "RDS 상태를 그대로
다시 내보내기"는 그 경합 자체가 없다.
"""

from __future__ import annotations

import json
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from cloudpathlib import S3Path

from src.common.config import (
    AWS_REGION,
    GOLD_CACHE_DIR,
    GOLD_SNAPSHOT_RETRY_BACKOFF_SECONDS,
    GOLD_SNAPSHOT_S3_CONNECT_TIMEOUT_SECONDS,
    GOLD_SNAPSHOT_S3_READ_TIMEOUT_SECONDS,
)
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="gold_snapshot")

# read_snapshot()이 응답 불가능한 S3에 무한정 기다리지 않게 거는 타임아웃.
# boto3 기본값(연결/읽기 각 60초)을 그대로 두면 이 폴백 계층 자체가
# "무조건 응답" 원칙을 못 지킨다(nav-api Lambda 전체 제한 시간이 10초).
# 정상 상황에서 GetObject는 보통 수백 ms 이내로 끝나므로 1초는 넉넉한
# 여유값이다 - 정확한 실측(p50/p99) 전까지의 시작값이라 재조정이 필요할
# 수 있어 config.py(환경변수)에서 가져온다.
_S3_CONNECT_TIMEOUT_SECONDS = GOLD_SNAPSHOT_S3_CONNECT_TIMEOUT_SECONDS
_S3_READ_TIMEOUT_SECONDS = GOLD_SNAPSHOT_S3_READ_TIMEOUT_SECONDS
_RETRY_BACKOFF_SECONDS = GOLD_SNAPSHOT_RETRY_BACKOFF_SECONDS

# 지연 생성 후 재사용한다(Lambda 웜스타트 사이에도) - 요청마다 클라이언트를
# 새로 만들 필요가 없다.
_s3_client = None


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client(
            "s3",
            region_name=AWS_REGION,
            config=Config(
                connect_timeout=_S3_CONNECT_TIMEOUT_SECONDS,
                read_timeout=_S3_READ_TIMEOUT_SECONDS,
                # 기본 재시도 정책을 그대로 두면 타임아웃마다 재시도가
                # 붙어 실제 대기 시간이 배로 늘어난다 - fallback 체인
                # 자체가 "다음 단계로 넘어가기"라는 재시도 역할을 하므로
                # 여기서는 재시도 없이 1회만 시도한다.
                retries={"max_attempts": 1},
            ),
        )
    return _s3_client


def snapshot_path(type_name: str):
    return GOLD_CACHE_DIR / f"{type_name}_snapshot.json"


def write_snapshot(type_name: str, snapshot: dict[str, dict]) -> None:
    """snapshot은 segment_id -> {"avg", "spec", "exact_value", "exact_observed_at"}
    매핑(각 키는 값이 있을 때만 존재).

    read_snapshot()과 동일하게 S3 경로는 타임아웃을 직접 건 boto3
    클라이언트로 쓴다 - 호출부(nav_time/gold2.py의 write_to_rds)가 이미
    이 쓰기를 실패해도 파이프라인을 안 죽이는 best-effort로 다루지만,
    S3가 응답 없을 때 boto3 기본값(60초)만큼 task를 붙잡아두는 것 자체를
    막기 위함이다. 읽기와 같은 크기(세그먼트당 최신값 1개)의 데이터라
    같은 타임아웃 값을 재사용한다."""
    path = snapshot_path(type_name)
    payload = json.dumps(snapshot)

    if isinstance(path, S3Path):
        _get_s3_client().put_object(Bucket=path.bucket, Key=path.key, Body=payload.encode("utf-8"))
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload)

    logger.info(f"[gold_snapshot] {type_name} 스냅샷 저장 완료: {len(snapshot)}개 세그먼트 -> {path}")


class LazySnapshot:
    """S3 Gold 스냅샷을 메모리에 캐싱한다 - 성공(hit)하면 프로세스 수명
    동안 재사용하고, 최초 로드가 실패(missing/error)하면 backoff를 두고
    다음 기회에 다시 시도한다.

    서빙 쪽(nav_lookup.py/toll/serving.py/api.py)이 각자 "_xxx_loaded bool +
    _xxx dict 전역변수 + _load_xxx_once() 함수" 형태로 반복 구현하던 걸
    공용화한 것 - 세그먼트마다 매번 새로 읽으면 RDS가 죽어있는 동안
    세그먼트 수만큼 S3 호출이 쌓이는 문제가 재발하므로(Lambda 웜
    인스턴스에서 최초 미스 때 한 번만 로드해 재사용해야 함), 그 lazy-load
    자체는 타입마다 동일한 모양이라 여기로 뽑았다. fallback 판단(어떤
    순서로 어떤 값을 쓸지)은 호출부마다 다르므로 그대로 남겨둔다.

    예전엔 결과에 상관없이 최초 1회만 읽고 `_loaded=True`로 고정했는데,
    첫 S3 읽기가 일시 장애로 `{}`를 반환하면 그 빈 결과가 프로세스 수명
    내내 캐시되는 버그가 있었다(RELIABILITY_PRINCIPLES.md 열린 질문).
    이제 hit만 고정하고, 실패는 `_RETRY_BACKOFF_SECONDS` 창 뒤에 다시
    시도한다 - 그동안은 S3를 다시 두드리지 않고(요청당 타임아웃/호출 폭주
    방지) 현재 캐시를 준다. 성공 후 주기적 refresh(TTL)까지는 넣지 않는다."""

    def __init__(self, type_name: str) -> None:
        self._type_name = type_name
        # hit을 한 번 받으면 True - 그 뒤로는 S3를 다시 안 친다(TTL refresh 없음).
        self._loaded = False
        self._data: dict[str, dict] = {}
        # 마지막 miss(파일 없음/읽기 실패) 시각(monotonic). backoff 계산용.
        self._last_failed_at = 0.0

    def get(self) -> dict[str, dict]:
        if self._loaded:
            return self._data

        # 최초 로드가 실패(missing/error)한 뒤엔 요청마다 S3를 다시 두드리지
        # 않고 backoff만큼 기다린다 - 그동안은 현재 캐시(보통 {})를 준다.
        now = time.monotonic()
        if now - self._last_failed_at < _RETRY_BACKOFF_SECONDS:
            return self._data

        data, status = read_snapshot_result(self._type_name)
        if status == "hit":
            self._data = data
            self._loaded = True
        else:
            # miss - _loaded는 False로 두고 backoff 창을 연다. 이미 유효한
            # 캐시(hit)가 있으면 miss로 덮어쓰지 않는다(self._data 유지).
            self._last_failed_at = now
        return self._data


def export_best_effort(type_name: str, build_snapshot, logger, log_prefix: str) -> None:
    """Gold 파이프라인이 RDS 쓰기 성공 후 S3 스냅샷을 최선 노력(best-effort)으로
    갱신한다 - nav_length/gold2.py, nav_time/gold2.py, toll/gold.py 세
    write_to_rds류 함수가 각자 구현하던 동일한 try/except/log 블록을
    공용화한 것. 스냅샷 갱신 자체가 실패해도 RDS 쓰기는 이미 끝난 뒤라
    파이프라인을 실패시키지 않는다(다음 정상 실행 때 다시 시도되면
    충분하다) - 그래서 예외를 여기서 삼키고 로깅만 한다.

    build_snapshot은 인자 없는 콜러블이다 - 단순 dict 컴프리헨션(nav_length/
    toll)이 아니라 RDS를 다시 조회해서 원본을 뽑는 경우(nav_time/gold2.py의
    _export_snapshot)도 있어서, 그 빌드 단계 자체의 실패도 이 함수의
    best-effort 범위 안에 포함시킨다(원본 구현이 try 블록 안에서 빌드까지
    같이 하던 것과 동일한 범위)."""
    try:
        snapshot = build_snapshot()
        write_snapshot(type_name, snapshot)
    except Exception:
        logger.exception(f"[{log_prefix}] S3 Gold 스냅샷 갱신 실패(RDS 쓰기 자체는 성공)")


def read_snapshot(type_name: str) -> dict[str, dict]:
    """스냅샷 파일이 없거나 읽기/파싱에 실패하면 빈 dict를 반환한다 -
    호출부(nav_lookup)가 이걸 "폴백도 못 씀"으로 처리해서 하드코딩
    상수로 넘어가게 한다. "무조건 응답" 원칙상 이 최후의 안전망에서
    예외를 던지면 안 된다.

    S3 경로는 cloudpathlib(S3Path)이 아니라 타임아웃을 직접 건 boto3
    클라이언트로 읽는다 - cloudpathlib은 커스텀 connect/read timeout을
    넣을 방법을 제공하지 않아서, 그대로 쓰면 boto3 기본값(60초)이 적용돼
    S3 자체가 느려지거나 응답 없을 때 이 폴백 계층이 무한정 걸린다."""
    path = snapshot_path(type_name)
    started = time.monotonic()
    try:
        if isinstance(path, S3Path):
            try:
                response = _get_s3_client().get_object(Bucket=path.bucket, Key=path.key)
            except ClientError as exc:
                error_code = exc.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchKey", "404"):
                    return {}
                raise
            body = response["Body"].read()
        else:
            if not path.exists():
                return {}
            body = path.read_bytes()
        return json.loads(body)
    except Exception:
        logger.exception(f"[gold_snapshot] {type_name} 스냅샷 읽기 실패")
        return {}
    finally:
        logger.info(
            f"[gold_snapshot] {type_name} 스냅샷 읽기 소요 시간: "
            f"{(time.monotonic() - started) * 1000:.0f}ms"
        )


def read_snapshot_result(type_name: str) -> tuple[dict[str, dict], str]:
    """read_snapshot()을 호출하고 결과를 hit/miss로 분류한다.

      - "hit":  비지 않은 데이터를 정상적으로 읽음 → LazySnapshot이 프로세스
                수명 동안 캐시한다.
      - "miss": 파일 없음 / 읽기·파싱 실패 / (드물게) 실제로 빈 스냅샷 →
                LazySnapshot이 backoff 후 다시 시도한다.

    파일 없음(missing)과 일시 장애(error)를 더 세분하지 않는 이유: 둘 다
    "잠시 뒤 재시도"로 동일하게 처리하기 때문이다. 관측용으로 구분이
    필요하면 read_snapshot() 내부 로그로 충분하고, 세분류는 스냅샷
    envelope(generated_at/checksum) 후속 작업으로 넘긴다."""
    data = read_snapshot(type_name)
    return (data, "hit") if data else ({}, "miss")
