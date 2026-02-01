from util.paths import FAILED_DIR, SQL_DIR
from oracle.export_csv import export_oracle_to_csv

MAX_RETRY = 3

def retry_failed_sql(host_name, host_cfg, params, batch_date):
    fail_file = FAILED_DIR / f"{host_name}.lst"
    if not fail_file.exists():
        return []

    all_sql = {p.name: p for p in SQL_DIR.rglob("*.sql")}
    failed = fail_file.read_text().splitlines()

    for _ in range(MAX_RETRY):
        new_failed = []
        for name in failed:
            try:
                export_oracle_to_csv(host_name, host_cfg, [all_sql[name]], params, batch_date)
            except Exception:
                new_failed.append(name)

        if not new_failed:
            fail_file.unlink()
            return []

        fail_file.write_text("\n".join(new_failed))
        failed = new_failed

    return failed
