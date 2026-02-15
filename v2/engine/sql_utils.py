# file: v2/engine/sql_utils.py

import re
from pathlib import Path

SQL_PREFIX_PATTERN = re.compile(r"^(\d+)_.*\.sql$", re.IGNORECASE)


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

    # prefix 없으면 파일명 정렬
    return sorted(files, key=lambda f: f.name.lower())
