"""
RDS(Postgres) 연결 모듈

Gold 서빙 테이블(대시보드/API가 실시간으로 읽는 테이블)을 RDS에 쓰고
읽는다. S3는 계속 데이터 레이크(적재 이력)로 남고, RDS는 그중 서빙에
필요한 최종 결과만 별도로 들고 있는 서빙 레이어다.
"""

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.common.config import RDS_DB, RDS_HOST, RDS_PASSWORD, RDS_PORT, RDS_USER

_engine: Engine | None = None


def get_engine() -> Engine:
    """RDS 연결 Engine을 반환한다 (프로세스당 한 번만 생성해서 재사용)."""

    global _engine

    if _engine is None:
        _engine = create_engine(
            f"postgresql+psycopg2://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DB}"
        )

    return _engine


def write_table(df, table_name: str) -> None:
    """DataFrame으로 테이블 전체를 덮어쓴다.

    기존에 각 Gold 산출물을 parquet 파일 하나로 통째로 덮어쓰던 것과
    동일한 시맨틱이다(증분 upsert가 아니라 매번 전체 재생성).
    """

    df.to_sql(
        table_name,
        get_engine(),
        if_exists="replace",
        index=False,
    )


def read_table(table_name: str, columns: list[str] | None = None):
    """테이블 전체(또는 지정한 컬럼만)를 DataFrame으로 읽는다."""

    import pandas as pd

    column_list = ", ".join(columns) if columns else "*"

    return pd.read_sql(
        f"SELECT {column_list} FROM {table_name}",
        get_engine(),
    )
