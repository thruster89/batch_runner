def expand_param_value(value: str) -> list[str]:
    """
    단일 / 리스트 / yyyymm 범위 처리
    """
    value = value.strip()

    # range: 202403:202406
    if ":" in value:
        start, end = value.split(":")
        return _expand_yymm_range(start, end)

    # list: A,B,C
    if "," in value:
        return [v.strip() for v in value.split(",") if v.strip()]

    return [value]


def _expand_yymm_range(start: str, end: str) -> list[str]:
    res = []
    y, m = int(start[:4]), int(start[4:])
    ey, em = int(end[:4]), int(end[4:])

    while (y < ey) or (y == ey and m <= em):
        res.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return res
