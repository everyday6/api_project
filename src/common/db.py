"""
RDS(Postgres) 연결 모듈

Gold 서빙 테이블(대시보드/API가 실시간으로 읽는 테이블)을 RDS에 쓰고
읽는다. S3는 계속 데이터 레이크(적재 이력)로 남고, RDS는 그중 서빙에
필요한 최종 결과만 별도로 들고 있는 서빙 레이어다.
"""

import pandas as pd
from sqlalchemy import create_engine, inspect, text
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


def table_exists(table_name: str) -> bool:
    """테이블이 아직 한 번도 안 만들어졌을 수 있는 경우(수동 트리거 DAG가
    아직 안 돌았을 때 등) 확인용."""

    return inspect(get_engine()).has_table(table_name)


def read_table(table_name: str, columns: list[str] | None = None) -> pd.DataFrame:
    """테이블 전체(또는 지정한 컬럼만)를 DataFrame으로 읽는다."""

    column_list = ", ".join(columns) if columns else "*"

    return pd.read_sql(
        f"SELECT {column_list} FROM {table_name}",
        get_engine(),
    )


# =========================================================
# dt= 파티션 테이블 (map_road_control_segment 등 — S3에서 날짜별
# 스냅샷을 나란히 보관하던 것들)
#
# RDS에서는 파일 디렉터리 구조 대신 "dt" 컬럼으로 같은 걸 흉내낸다:
# 같은 dt로 다시 쓰면 그 날짜 분만 지우고 다시 넣는다(파티션 파일을
# 통째로 덮어쓰던 것과 동일한 재실행 안전성), 다른 dt는 계속 보존한다.
# =========================================================

def write_partitioned_table(df: pd.DataFrame, table_name: str, dt: str) -> None:
    """dt 파티션 하나를 쓴다. 같은 dt로 재실행되면 그 날짜분만 교체한다."""

    engine = get_engine()

    with engine.begin() as conn:
        if inspect(engine).has_table(table_name):
            conn.execute(
                text(f'DELETE FROM {table_name} WHERE dt = :dt'),
                {"dt": dt},
            )

        frame = df.copy()
        frame["dt"] = dt
        frame.to_sql(table_name, conn, if_exists="append", index=False)


def latest_partition_date(table_name: str) -> str | None:
    """table_name에 있는 dt 중 가장 최근 날짜. 테이블이 아직 없거나 비어있으면 None.

    S3의 `base_dir.glob("dt=*/data.parquet")`으로 최신 파티션을 찾던 것과
    동일한 역할이다.
    """

    engine = get_engine()
    if not inspect(engine).has_table(table_name):
        return None

    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT MAX(dt) FROM {table_name}")).scalar()

    return result


def read_partition(table_name: str, dt: str, columns: list[str] | None = None) -> pd.DataFrame:
    """table_name의 특정 dt 파티션만 DataFrame으로 읽는다 (최신이 아니어도 됨)."""

    engine = get_engine()
    if not inspect(engine).has_table(table_name):
        return pd.DataFrame(columns=columns or [])

    column_list = ", ".join(columns) if columns else "*"

    return pd.read_sql(
        f"SELECT {column_list} FROM {table_name} WHERE dt = %(dt)s",
        engine,
        params={"dt": dt},
    )


def read_latest_partition(table_name: str, columns: list[str] | None = None) -> pd.DataFrame:
    """table_name의 가장 최근 dt 파티션만 DataFrame으로 읽는다."""

    dt = latest_partition_date(table_name)
    if dt is None:
        return pd.DataFrame(columns=columns or [])

    column_list = ", ".join(columns) if columns else "*"

    return pd.read_sql(
        f"SELECT {column_list} FROM {table_name} WHERE dt = %(dt)s",
        get_engine(),
        params={"dt": dt},
    )
