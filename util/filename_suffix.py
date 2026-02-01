# util/filename_suffix.py
from pathlib import Path
import re

SUFFIX_PATTERN = re.compile(r"__(.+)$")


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


def extract_param_suffix(filename: str) -> str:
    """
    a1__clsYymm=202403.parquet -> __clsYymm=202403
    a1.parquet               -> ""
    """
    name = filename.rsplit(".", 1)[0]
    m = SUFFIX_PATTERN.search(name)
    return m.group(0) if m else ""


# def split_table_and_suffix(filename: str) -> tuple[str, str]:
#     """
#     a1__clsYymm=202403.parquet -> ("A1", "__clsYymm=202403")
#     a1.parquet                -> ("A1", "")
#     """
#     stem = Path(filename).stem
#     m = SUFFIX_PATTERN.search(stem)

#     if not m:
#         return stem.upper(), ""

#     table = stem[: m.start()]
#     suffix = m.group(0)
#     return table.upper(), suffix

def split_table_and_suffix(name: str) -> tuple[str, str]:
    """
    a1__clsYymm=202312 → ("A1", "__clsYymm=202312")
    rate               → ("RATE", "")
    """
    if "__" in name:
        base, suffix = name.split("__", 1)
        return base.upper(), "__" + suffix
    return name.upper(), ""
