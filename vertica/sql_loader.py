from pathlib import Path
from util.paths import SQL_DIR

def collect_sql_files_for_schema(schema: str) -> list[Path]:
    base = SQL_DIR / schema
    if not base.exists():
        raise RuntimeError(f"SQL directory not found: {base}")
    return list(base.rglob("*.sql"))


def collect_sql_files(
    schema: str,
    subdirs: list[str] | None = None,
) -> list[Path]:
    """
    schema 하위 SQL 수집
    subdirs 지정 시 해당 하위 경로만 포함
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
        rel = p.relative_to(base)          # A/a1.sql
        sub = rel.parent.as_posix()        # A
        if sub in allow:
            filtered.append(p)

    return filtered