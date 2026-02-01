import re
from typing import Dict


# =========================================================
# SUFFIX BUILD
# =========================================================

def build_param_suffix(params: dict, keys: set | list) -> str:
    """
    params : 실행 시점의 실제 파라미터 값
    keys   : 이 SQL에서 실제 사용된 파라미터 이름들
    """
    if not keys:
        return ""

    parts = []
    for k in sorted(keys):
        v = params.get(k)
        if v is None:
            continue
        parts.append(f"{k}={v}")

    if not parts:
        return ""

    return "__" + "_".join(parts)



# =========================================================
# SUFFIX EXTRACT
# =========================================================

_SUFFIX_PATTERN = re.compile(r"(__[A-Za-z0-9_]+=.+?)(?=\.|$)")


def extract_param_suffix(filename: str) -> str:
    """
    파일명에서 suffix 추출

    예:
        a1__clsYymm=202403.parquet → "__clsYymm=202403"
        a2__clsYymm=202403__prodCd=A01.csv.gz
            → "__clsYymm=202403__prodCd=A01"
        rate.parquet → ""
    """
    m = _SUFFIX_PATTERN.search(filename)
    return m.group(1) if m else ""
