"""서빙 응답의 "값 출처"를 두 축으로 나눈 provenance.

기존 `sources`(평면 문자열: fresh/avg/rds/global/snapshot/hardcoded)는 축이
섞여 있었다 - rds/snapshot은 "어느 저장소에서", fresh/avg는 "어떻게 만든 값",
global은 "어떤 대체 전략", hardcoded는 둘 다다. 특히 type1이 S3 스냅샷으로
떨어져도 응답엔 fresh/avg만 보여 저장소 provenance가 사라졌고, type4는
"RDS에서 0을 읽음"과 "RDS엔 살아있으나 행이 없어 0으로 추론함"이 똑같이
rds로 보였다.

이제 resolver가 `{storage_source, value_basis}` 구조를 반환하고, 기존
`sources`는 여기서 파생한다(단일 소스 - 두 값을 따로 계산하지 않는다).
resolver 내부는 `"<storage>:<basis>"` 토큰 문자열로 다루다가 마지막에
`to_provenance()`로 구조화한다(fallback 체인 코드 변경을 최소화하려고).
"""

from __future__ import annotations

# storage_source: 값이 물리적으로 어디서 왔나.
STORAGE_RDS = "rds"
STORAGE_MEMORY_CACHE = "memory_cache"
STORAGE_S3_SNAPSHOT = "s3_snapshot"
STORAGE_CODE = "code"

# value_basis: 그 값이 어떻게 만들어졌나.
BASIS_OBSERVED = "observed"                       # type1: 오늘의 실측 통과시간
BASIS_HISTORICAL_AVERAGE = "historical_average"   # type1: 슬롯 과거 평균
BASIS_SEGMENT_VALUE = "segment_value"             # type2/4: 그 세그먼트의 저장된 값
BASIS_GLOBAL_DEFAULT = "global_default"           # type2: 전체 GLOBAL 기본값
BASIS_MODELED_AGGREGATE = "modeled_aggregate"     # type3: rolling 요일×시간 평균
BASIS_IMPLICIT_ZERO = "implicit_zero"             # type4: 통행료 대상 아님 -> 0
BASIS_STATIC_DEFAULT = "static_default"           # 어느 타입이든 코드 상수


def token(storage: str, basis: str) -> str:
    return f"{storage}:{basis}"


def to_provenance(tokens: list[str]) -> list[dict]:
    """resolver가 쌓은 `"<storage>:<basis>"` 토큰 리스트를 구조체 리스트로."""
    result = []
    for tok in tokens:
        storage, _, basis = tok.partition(":")
        result.append({
            "storage_source": storage,
            "value_basis": basis or BASIS_STATIC_DEFAULT,
        })
    return result


# provenance -> 기존 `sources` 문자열(하위 호환). type마다 어휘가 다르다.
_LEGACY = {
    1: {
        (STORAGE_RDS, BASIS_OBSERVED): "fresh",
        (STORAGE_MEMORY_CACHE, BASIS_OBSERVED): "fresh",
        (STORAGE_S3_SNAPSHOT, BASIS_OBSERVED): "fresh",
        (STORAGE_RDS, BASIS_HISTORICAL_AVERAGE): "avg",
        (STORAGE_MEMORY_CACHE, BASIS_HISTORICAL_AVERAGE): "avg",
        (STORAGE_S3_SNAPSHOT, BASIS_HISTORICAL_AVERAGE): "avg",
    },
    2: {
        (STORAGE_RDS, BASIS_SEGMENT_VALUE): "rds",
        (STORAGE_RDS, BASIS_GLOBAL_DEFAULT): "global",
        (STORAGE_S3_SNAPSHOT, BASIS_SEGMENT_VALUE): "snapshot",
        (STORAGE_S3_SNAPSHOT, BASIS_GLOBAL_DEFAULT): "hardcoded",
    },
    3: {
        (STORAGE_RDS, BASIS_MODELED_AGGREGATE): "rds",
        (STORAGE_S3_SNAPSHOT, BASIS_MODELED_AGGREGATE): "snapshot",
    },
    4: {
        (STORAGE_RDS, BASIS_SEGMENT_VALUE): "rds",
        (STORAGE_RDS, BASIS_IMPLICIT_ZERO): "rds",
        (STORAGE_S3_SNAPSHOT, BASIS_SEGMENT_VALUE): "snapshot",
    },
}


def legacy_source(type_: int, prov: dict) -> str:
    """구조화된 provenance 하나를 기존 `sources` 문자열로 투영한다.
    매핑에 없는 조합(대부분 code:*)은 전부 "hardcoded"로 떨어진다."""
    key = (prov["storage_source"], prov["value_basis"])
    return _LEGACY.get(type_, {}).get(key, "hardcoded")


def legacy_sources(type_: int, provenance: list[dict]) -> list[str]:
    return [legacy_source(type_, prov) for prov in provenance]
