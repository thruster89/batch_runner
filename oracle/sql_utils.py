import re

PARAM_PATTERNS = [
    re.compile(r":([A-Za-z_][A-Za-z0-9_]*)"),
    re.compile(r"\{\#([A-Za-z_][A-Za-z0-9_]*)\}"),
    re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}"),
]

def normalize_sql(sql: str) -> str:
    sql = sql.strip()
    if sql.endswith(";"):
        sql = sql[:-1]
    sql = sql.replace("\n/", "\n")
    return sql

def extract_params(sql: str) -> set[str]:
    found = set()
    for p in PARAM_PATTERNS:
        found |= set(p.findall(sql))
    return found

def apply_params(sql: str, params: dict) -> str:
    for k, v in params.items():
        v = str(v)
        sql = (
            sql.replace(f":{k}", f"'{v}'")
               .replace(f"{{#{k}}}", f"'{v}'")
               .replace(f"${{{k}}}", f"'{v}'")
        )
    return sql
