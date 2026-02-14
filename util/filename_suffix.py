# util/filename_suffix.py
from pathlib import Path
import re

SUFFIX_PATTERN = re.compile(r"_(\d+)$")


def build_param_suffix(params: dict, keys: set | list) -> str:
    """
    첫 번째 파라미터 값만 suffix로 사용
    예:
      clsYymm=202312 -> _202312
    """
    if not keys:
        return ""

    # first_key = sorted(keys)[0]
    PRIORITY_KEYS = ["clsYymm", "baseYymm"]

    for k in PRIORITY_KEYS:
        if k in keys:
            first_key = k
            break
    else:
        first_key = sorted(keys)[0]
    
    
    value = params.get(first_key)

    if value is None:
        return ""

    # 파일명 안전 처리
    value = str(value)
    value = re.sub(r"[^\w\-]", "", value)

    return f"_{value}"


def extract_param_suffix(filename: str) -> str:
    """
    a1_202312.parquet -> _202312
    a1.parquet        -> ""
    """
    name = filename.rsplit(".", 1)[0]
    m = SUFFIX_PATTERN.search(name)
    return m.group(0) if m else ""


def split_table_and_suffix(name: str) -> tuple[str, str]:
    """
    a1_202312 → ("A1", "_202312")
    rate      → ("RATE", "")
    """
    m = SUFFIX_PATTERN.search(name)

    if not m:
        return Path(name).stem.upper(), ""

    base = name[: m.start()]
    suffix = m.group(0)

    return base.upper(), suffix
