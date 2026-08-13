"""공통 유틸."""

import requests
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def make_session():
    """네트워크 오류 시 자동 재시도하는 세션."""
    session = requests.Session()

    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    session.mount(
        "https://",
        HTTPAdapter(max_retries=retry),
    )

    return session


def save_parquet(df, out_dir, filename="data.parquet"):
    """임시 파일로 쓰고 성공하면 이름을 바꾼다."""
    out_dir.mkdir(parents=True, exist_ok=True)

    tmp = out_dir / f"_tmp_{filename}"
    final = out_dir / filename

    df.to_parquet(tmp, index=False)
    tmp.replace(final)

    return final

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
 