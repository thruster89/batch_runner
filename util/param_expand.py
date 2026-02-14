import re

YYYYMM_PATTERN = re.compile(r"^\d{6}$")
RANGE_PATTERN  = re.compile(r"^\d{6}:\d{6}$")


def expand_param_value(value: str) -> list[str]:
    """
    단일 / 리스트 / YYYYMM 범위 처리
    """

    value = value.strip()

    # YYYYMM range
    if RANGE_PATTERN.match(value):
        start, end = value.split(":")
        return _expand_yymm_range(start, end)

    # list
    if "," in value:
        return [v.strip() for v in value.split(",") if v.strip()]

    # 단일 값
    return [value]


def _expand_yymm_range(start: str, end: str) -> list[str]:

    _validate_yymm(start)
    _validate_yymm(end)

    sy, sm = int(start[:4]), int(start[4:])
    ey, em = int(end[:4]), int(end[4:])

    if (sy, sm) > (ey, em):
        raise ValueError(f"Invalid range: {start}:{end}")

    res = []

    y, m = sy, sm

    while (y < ey) or (y == ey and m <= em):
        res.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1

    return res


def _validate_yymm(val: str):

    if not YYYYMM_PATTERN.match(val):
        raise ValueError(f"Invalid YYYYMM format: {val}")

    month = int(val[4:])
    if not 1 <= month <= 12:
        raise ValueError(f"Invalid month in YYYYMM: {val}")
