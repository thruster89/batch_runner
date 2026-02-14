import hashlib
from pathlib import Path


def compute_sql_hash(sql_text: str) -> str:
    """
    SQL 문자열 기준 hash 생성
    """
    h = hashlib.md5()
    h.update(sql_text.encode("utf-8"))
    return h.hexdigest()