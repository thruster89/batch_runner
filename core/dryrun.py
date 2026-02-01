import pandas as pd
from oracle.sql_utils import extract_params
from util.paths import LOG_DIR

def dryrun_check(host_name, sql_files, params, batch_ts):
    rows = []
    for sql_file in sql_files:
        sql = sql_file.read_text(encoding="utf-8")
        issues = []

        if sql.strip().endswith(";"):
            issues.append("ends with ';'")
        if "\n/" in sql:
            issues.append("contains '/'")

        used = extract_params(sql)
        missing = used - params.keys()

        if missing:
            issues.append(f"missing params {sorted(missing)}")

        rows.append({
            "batch_ts": batch_ts,
            "host": host_name,
            "sql_file": sql_file.name,
            "status": "OK" if not issues else "FAIL",
            "issues": "; ".join(issues),
            "params_used": ",".join(sorted(used)),
            "params_missing": ",".join(sorted(missing)),
        })
    return rows

def write_dryrun_report(rows, batch_ts):
    out = LOG_DIR / f"dryrun_report_{batch_ts}.csv"
    pd.DataFrame(rows).to_csv(out, index=False, encoding="utf-8-sig")
