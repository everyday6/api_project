"""
TicketMaster venue 차원

TicketMaster API는 수용 인원을 주지 않으므로 직접 관리한다.

왜 정규화가 필요한가:
같은 장소가 여러 표기로 등록돼 있다 (실제 데이터에서 27쌍 확인).
    Broadway Theatre-New York  /  Broadway Theatre
    Bernard B. Jacobs Theatre  /  Jacobs Theatre-NY
    Jacob Javits Center        /  Javits Convention Center
정규화 없이 이름을 그대로 쓰면 같은 장소가 둘로 갈려
venue별 집계가 어긋나고 가중치도 조용히 누락된다.

수용 인원은 공개된 좌석 수 기준의 추정값이며 팀 검증 대상이다.
가중치 계산은 Gold에서 한다. 여기서는 사실(수용 인원)만 관리한다.
"""

from __future__ import annotations

import re

from common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="ticketmaster_venue")

# 수용 인원을 모르는 venue의 기본값.
# 소규모 클럽/극장이 대부분이므로 보수적으로 잡는다.
DEFAULT_CAPACITY = 200


# =========================================================
# 이름 정규화
# =========================================================

# TicketMaster가 같은 장소에 붙이는 지역 접미어
SUFFIX_PATTERNS = [
    r"\s*-\s*NY$",
    r"\s*-\s*NYC$",
    r"\s*-\s*NEW YORK$",
    r"\s+NEW YORK$",
    r"\s+NYC$",
]

# 접미어 규칙으로 처리되지 않는 개별 사례
MANUAL_ALIASES = {
    "JACOBS THEATRE": "BERNARD B JACOBS THEATRE",
    "STERN AUDITORIUM / PERELMAN STAGE AT CARNEGIE HALL": "CARNEGIE HALL",
    "JACOB JAVITS CENTER": "JAVITS CONVENTION CENTER",
    "IRVING PLAZA POWERED BY VERIZON 5G": "IRVING PLAZA",
    "NIGHTCLUB 101": "NIGHT CLUB 101",
    "DR2": "DR2 THEATRE",
    "RACKET": "RACKET NYC",
    "HILL COUNTRY LIVE": "HILL COUNTRY",
    "HILL COUNTRY NYC": "HILL COUNTRY",
    "HARD ROCK CAFE": "HARD ROCK CAFE NYC",
    "LINCOLN CENTER - VIVIAN BEAUMONT": "LINCOLN CENTER - VIVIAN BEAUMONT THEATRE",
    "LINCOLN CENTER - MITZI E NEWHOUSE": "LINCOLN CENTER - MITZI E NEWHOUSE THEATRE",
    "CIRCLE LINE CRUISES, PIER 83": "CIRCLE LINE CRUISES",
}


def normalize_venue(value) -> str | None:
    """venue 이름을 표준 표기로 정규화한다."""

    if not isinstance(value, str):
        return None

    name = re.sub(r"\s+", " ", value).strip().upper()

    if not name:
        return None

    # 마침표/아포스트로피 제거 (Samuel J. , Randall's)
    name = name.replace(".", "").replace("'", "").replace("\u2019", "")

    # 하이픈 간격 통일 (Theatre-NY, Theatre - NY, Center-  Mitzi)
    name = re.sub(r"\s*-\s*", " - ", name)

    # 표기 통일 (Theater / Theatre)
    name = re.sub(r"\bTHEATER\b", "THEATRE", name)

    for pattern in SUFFIX_PATTERNS:
        name = re.sub(pattern, "", name)

    name = re.sub(r"\s+", " ", name).strip()

    return MANUAL_ALIASES.get(name, name)


# =========================================================
# 제외 대상
# =========================================================

# 공연이 아니라 상시 전시/투어/유람선이라 교통 영향 성격이 다르다.
# BANKSY MUSEUM은 관람 시간대를 슬롯으로 쪼개 등록해
# 전체 이벤트의 약 19%(2,435건)를 차지하지만 실제 부하는 작다.
EXCLUDE_VENUES = {
    "BANKSY MUSEUM",
    "RADIO CITY MUSIC HALL TOUR EXPERIENCE",
    "MUSEUM OF CHINESE IN AMERICA",
    "ROCKS OFF CONCERT CRUISE SERIES",
    "THE LIBERTY BELLE - ROCKS OFF CONCERT CRUISE",
    "THE COSMO - ROCKS OFF CONCERT CRUISE",
    "CIRCLE LINE CRUISES",
    "SKYPORT MARINA",
    "JOANNE TRATTORIA",
}


# =========================================================
# 수용 인원
# =========================================================
#
# 정규화된 이름 -> 수용 인원(석)
# verified 여부를 코드로 관리하지 않으므로,
# 값을 확인/수정할 때는 커밋 메시지에 근거를 남긴다.

VENUE_CAPACITY = {
    # --- arena ---
    "MADISON SQUARE GARDEN": 20000,

    # --- stadium ---
    "LAWRENCE A WIEN STADIUM": 17000,
    "ICAHN STADIUM": 5000,

    # --- large_hall ---
    "RADIO CITY MUSIC HALL": 6000,
    "METROPOLITAN OPERA HOUSE": 3800,
    "UNITED PALACE": 3400,
    "BEACON THEATRE": 2900,
    "CARNEGIE HALL": 2800,
    "DAVID H KOCH THEATRE": 2600,
    "NEW YORK CITY CENTER": 2250,
    "DAVID GEFFEN HALL": 2200,
    "TOWN HALL": 1500,
    "INFOSYS THEATRE AT MADISON SQUARE GARDEN": 1000,

    # --- broadway ---
    "GERSHWIN THEATRE": 1930,
    "BROADWAY THEATRE": 1760,
    "PALACE THEATRE": 1740,
    "ST JAMES THEATRE": 1710,
    "NEW AMSTERDAM THEATRE": 1700,
    "MAJESTIC THEATRE": 1645,
    "MINSKOFF THEATRE": 1620,
    "LYRIC THEATRE": 1620,
    "MARQUIS THEATRE": 1610,
    "WINTER GARDEN THEATRE": 1530,
    "LUNT - FONTANNE THEATRE": 1510,
    "NEIL SIMON THEATRE": 1470,
    "SHUBERT THEATRE": 1460,
    "IMPERIAL THEATRE": 1420,
    "AL HIRSCHFELD THEATRE": 1420,
    "RICHARD RODGERS THEATRE": 1400,
    "NEDERLANDER THEATRE": 1235,
    "AUGUST WILSON THEATRE": 1230,
    "BROADHURST THEATRE": 1190,
    "AMBASSADOR THEATRE": 1125,
    "EUGENE ONEILL THEATRE": 1110,
    "LONGACRE THEATRE": 1095,
    "JAMES EARL JONES THEATRE": 1090,
    "GERALD SCHOENFELD THEATRE": 1080,
    "LINCOLN CENTER - VIVIAN BEAUMONT THEATRE": 1080,
    "BERNARD B JACOBS THEATRE": 1078,
    "LENA HORNE THEATRE": 1070,
    "STEPHEN SONDHEIM THEATRE": 1055,
    "MUSIC BOX THEATRE": 1025,
    "BELASCO THEATRE": 1016,
    "STUDIO 54": 1006,
    "WALTER KERR THEATRE": 975,
    "HUDSON THEATRE": 970,
    "LYCEUM THEATRE": 920,
    "JOHN GOLDEN THEATRE": 800,
    "CIRCLE IN THE SQUARE THEATRE": 776,
    "TODD HAIMES THEATRE": 740,
    "SAMUEL J FRIEDMAN THEATRE": 650,
    "HELEN HAYES THEATRE": 597,

    # --- off_broadway ---
    "NEW WORLD STAGES - STAGE 1": 499,
    "NEW WORLD STAGES - STAGE 3": 499,
    "LAURA PELS THEATRE": 424,
    "AUDIBLES MINETTA LANE THEATRE": 390,
    "NEW WORLD STAGES - STAGE 4": 350,
    "NEW WORLD STAGES - STAGE 2": 350,
    "ORPHEUM THEATRE": 347,
    "DARYL ROTH THEATRE": 299,
    "PUBLIC THEATRE - NEWMAN THEATRE": 299,
    "LINCOLN CENTER - MITZI E NEWHOUSE THEATRE": 299,
    "THE LORETO THEATRE AT THE SHEEN CENTER FOR THOUGHT & CULTURE": 270,
    "WESTSIDE THEATRE UPSTAIRS": 249,
    "NEW WORLD STAGES - STAGE 5": 199,
    "THE THEATRE CENTER": 199,
    "THE RUBY THEATRE": 199,
    "LUCILLE LORTEL THEATRE": 199,
    "LA MAMA": 199,
    "THEATRE 555": 199,
    "CHERRY LANE THEATRE": 179,
    "ST LUKES THEATRE": 178,
    "GREENWICH HOUSE THEATRE": 140,
    "LINCOLN CENTER - CLAIRE TOW THEATRE": 112,
    "DR2 THEATRE": 99,

    # --- music_venue ---
    "TERMINAL 5": 3000,
    "MANHATTAN CENTER HAMMERSTEIN BALLROOM": 2200,
    "WEBSTER HALL": 1500,
    "IRVING PLAZA": 1200,
    "SONY HALL": 1000,
    "LE POISSON ROUGE": 700,
    "GRAMERCY THEATRE": 650,
    "BOWERY BALLROOM": 575,
    "RACKET NYC": 500,
    "MERCURY LOUNGE": 250,
    "CAFE WHA?": 250,
    "THE BITTER END": 230,
    "DROM": 200,
    "CUTTING ROOM": 200,
    "LUCINDAS": 150,
    "BLEECKER BELL": 150,
    "WILD HORSES": 150,
    "GROOVE": 150,
    "SUGAR MOUSE": 100,

    # --- jazz_club ---
    "BLUE NOTE JAZZ CLUB": 200,
    "IRIDIUM": 180,
    "BIRDLAND JAZZ CLUB": 150,
    "54 BELOW": 140,
    "BIRDLAND THEATRE": 100,
    "THE POCKET JAZZ CLUB": 100,

    # --- nightclub ---
    "PACHA": 500,
    "BOWERY PALACE": 300,
    "MASQUERADE": 300,
    "THE CULTURE CLUB": 200,
    "NIGHT CLUB 101": 200,
    "BERLIN": 200,

    # --- event_space ---
    "JAVITS CONVENTION CENTER": 10000,
    "CAPITALE": 1000,
    "THE VENUE @ HARD ROCK HOTEL NY": 500,
    "HK HALL": 200,
    "THE GREENE SPACE": 150,

    # --- outdoor ---
    "RANDALLS ISLAND": 10000,
    "CAPITAL ONE CITY PARKS FOUNDATION SUMMERSTAGE": 5000,
    "THE ROOFTOP AT PIER 17": 3000,
    "THE INKWELL HARBOR - CLUB ROOFTOP": 200,
    "PATIO AT RADIO HOTEL": 200,

    # --- restaurant ---
    "HARD ROCK CAFE NYC": 200,
    "HILL COUNTRY": 200,
    "ELLENS STARDUST THEATRE": 200,
}


def get_capacity(normalized_name) -> int:
    """정규화된 venue 이름의 수용 인원을 반환한다."""

    return VENUE_CAPACITY.get(normalized_name, DEFAULT_CAPACITY)


def attach_capacity(df, venue_col: str = "venue_name"):
    """
    DataFrame에 venue_name_norm / capacity / is_excluded 컬럼을 붙인다.

    가중치는 Gold에서 계산한다. 여기서는 수용 인원만 붙인다.
    """

    work = df.copy()

    work["venue_name_norm"] = work[venue_col].map(normalize_venue)

    work["capacity"] = (
        work["venue_name_norm"]
        .map(VENUE_CAPACITY)
        .fillna(DEFAULT_CAPACITY)
        .astype("int64")
    )

    work["is_excluded"] = work["venue_name_norm"].isin(EXCLUDE_VENUES)

    unknown = sorted(
        set(work["venue_name_norm"].dropna())
        - set(VENUE_CAPACITY)
        - EXCLUDE_VENUES
    )

    if unknown:
        logger.warning(
            "capacity 미확인 venue %d개 (기본값 %d 적용): %s",
            len(unknown),
            DEFAULT_CAPACITY,
            unknown[:10],
        )

    return work