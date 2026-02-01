from pathlib import Path
from typing import Iterable

from util.paths import SQL_DIR


def collect_sql_files(
    schema: str,
    subdirs: list[str] | None = None,
) -> list[Path]:
    """
    sql/<schema>/**/*.sql 수집
    subdirs가 있으면 해당 하위 폴더만
    """
    base = SQL_DIR / schema
    if not base.exists():
        raise RuntimeError(f"SQL directory not found: {base}")

    sql_files = list(base.rglob("*.sql"))

    if not subdirs:
        return sql_files

    allow = {s.strip().replace("\\", "/").strip("/") for s in subdirs}
    filtered = []

    for p in sql_files:
        rel = p.relative_to(base)
        sub = rel.parent.as_posix()
        if sub == ".":
            sub = ""
        if sub in allow:
            filtered.append(p)

    return filtered


def sql_files_to_tables(
    sql_files: Iterable[Path],
    schema: str,
) -> set[str]:
    """
    sql 파일 → DuckDB 테이블명 집합
    """
    tables = set()
    base = SQL_DIR / schema

    for p in sql_files:
        rel = p.relative_to(base)
        table = rel.stem.upper()
        tables.add(table)

    return tables
