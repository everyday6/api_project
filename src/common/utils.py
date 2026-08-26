"""공통 유틸."""

import re


def save_parquet(df, out_dir, filename="data.parquet"):
    """DataFrame을 parquet으로 저장한다.

    out_dir는 S3Path다 — S3는 업로드가 완료된 객체만 노출하므로(부분 쓰기가
    안 보임) 로컬 파일시스템에서 하던 tmp-then-rename 흉내가 필요 없다.
    pandas가 S3Path를 로컬 캐시 경로로 오해하지 않도록 str()로 넘긴다.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    final = out_dir / filename
    df.to_parquet(str(final), index=False)

    return final

def unique_in_order(values: list[str]) -> list[str]:
    """중복을 제거하되 처음 등장한 순서는 유지한다.

    서빙 조회부(nav_lookup.py/toll/serving.py/api.py)가 같은 segment_id로
    RDS를 여러 번 부르지 않으려고 각자 `list(dict.fromkeys(...))`를
    반복 구현하던 걸 공용화한 것 - dict는 3.7+부터 삽입 순서를 보장하므로
    이 트릭이 성립한다."""
    return list(dict.fromkeys(values))


def clean_street(value):
    """
    도로명 공백/대소문자를 정리한다.
 
    원본 데이터는 "WEST   19 STREET", "  12 STREET" 처럼
    앞뒤·중간 공백이 불규칙해 그대로 두면 JOIN이 실패한다.
    """
 
    if not isinstance(value, str):
        return None
 
    cleaned = re.sub(r"\s+", " ", value).strip().upper()
 
    return cleaned or None
 