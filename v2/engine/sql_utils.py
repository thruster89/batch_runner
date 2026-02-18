# file: v2/engine/sql_utils.py

import re
from pathlib import Path

SQL_PREFIX_PATTERN = re.compile(r"^(\d+)_.*\.sql$", re.IGNORECASE)
TABLE_HINT_PATTERN = re.compile(r"^--\[(.+)\]$")


def sort_sql_files(sql_dir: Path):
    files = list(sql_dir.glob("*.sql"))

    if not files:
        return []

    parsed = []
    prefix_found = False

    for f in files:
        m = SQL_PREFIX_PATTERN.match(f.name)
        if m:
            prefix_found = True
            order = int(m.group(1))
            parsed.append((order, f))
        else:
            parsed.append((None, f))

    if prefix_found:
        parsed.sort(key=lambda x: (x[0] is None, x[0], x[1].name))
        return [f for _, f in parsed]

    return sorted(files, key=lambda f: f.name.lower())


def resolve_table_name(sql_file: Path) -> str:
    """
    SQL 첫 줄(정확히는 첫 non-empty line)에 --[table_name] 이 있으면 그 값을 테이블명으로 사용.
    없으면 sql_file.stem 사용.
    """
    with open(sql_file, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue

            m = TABLE_HINT_PATTERN.match(s)
            if m:
                return m.group(1).strip()

            break

    return sql_file.stem


def extract_sqlname_from_csv(csv_path: Path) -> str:
    """
    csv 파일명 규칙: {sqlname}__{host}__{param}_{value}...
    여기서 sqlname은 첫 '__' 이전.

    .csv.gz의 경우 Path.stem이 '파일명.csv'가 되므로
    확장자를 직접 제거한 뒤 처리.
    """
    name = csv_path.name  # ex) 01_a1__local__clsYymm_202003.csv.gz

    # .csv.gz / .csv 둘 다 처리
    if name.endswith(".csv.gz"):
        stem = name[: -len(".csv.gz")]
    elif name.endswith(".csv"):
        stem = name[: -len(".csv")]
    else:
        stem = csv_path.stem

    return stem.split("__", 1)[0]