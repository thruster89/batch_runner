import time
import pandas as pd

from util.paths import SQL_DIR, CSV_DIR
from util.logging import get_host_logger
from util.param_expand import expand_param_value
from util.filename_suffix import build_param_suffix

from oracle.client import get_oracle_conn
from oracle.sql_utils import normalize_sql, extract_params, apply_params
from stats.slow_sql import SLOW_SQL_STATS

CHUNK_SIZE = 1_000_000


def export_oracle_to_csv(host_name, host_cfg, sql_files, params, batch_date):
    failed = []
    schema = host_cfg["duckdb_schema"]
    host_logger = get_host_logger(host_name, batch_date)

    for sql_file in sql_files:
        sql_start = time.time()

        try:
            rel = sql_file.relative_to(SQL_DIR / schema)
            subdir = rel.parent
            table = rel.stem

            out_dir = CSV_DIR / schema / subdir
            out_dir.mkdir(parents=True, exist_ok=True)

            sql_raw = normalize_sql(sql_file.read_text(encoding="utf-8"))
            used_keys = extract_params(sql_raw)

            # SQL에서 실제 사용된 파라미터만 확장
            expand_keys = sorted(used_keys)
            expand_values = [
                expand_param_value(str(params[k]))
                for k in expand_keys
            ] if expand_keys else [[]]

            cases = (
                zip(*expand_values)
                if expand_values != [[]]
                else [()]
            )

            for values in cases:
                case_params = params.copy()
                for k, v in zip(expand_keys, values):
                    case_params[k] = v

                suffix = build_param_suffix(case_params, expand_keys)
                out_file = out_dir / f"{table}{suffix}.csv.gz"

                if out_file.exists():
                    host_logger.info(
                        "CSV exists, skip export | %s",
                        out_file.as_posix(),
                    )
                    continue

                sql = apply_params(sql_raw, case_params)

                total_rows = 0
                start = time.time()

                with get_oracle_conn(host_cfg) as conn:
                    first = True
                    for chunk in pd.read_sql(sql, conn, chunksize=CHUNK_SIZE):
                        if chunk.empty:
                            continue

                        total_rows += len(chunk)
                        chunk.to_csv(
                            out_file,
                            mode="a",
                            header=first,
                            index=False,
                            compression="gzip",
                        )
                        first = False

                    conn.commit()

                elapsed = round(time.time() - start, 2)

                SLOW_SQL_STATS.append({
                    "host": host_name,
                    "sql_file": str(rel),
                    "elapsed_sec": elapsed,
                })

                param_desc = ", ".join(
                    f"{k}={case_params[k]}" for k in expand_keys
                ) or "-"

                if total_rows == 0:
                    host_logger.warning(
                        "CSV EMPTY | %s | %s | rows=0",
                        rel.as_posix(),
                        param_desc,
                    )
                else:
                    size_mb = out_file.stat().st_size / (1024 * 1024)
                    host_logger.info(
                        "CSV OK | %s | %s | rows=%d | %.2fMB | %.2fs",
                        rel.as_posix(),
                        param_desc,
                        total_rows,
                        size_mb,
                        elapsed,
                    )

        except Exception as e:
            elapsed = round(time.time() - sql_start, 2)
            host_logger.error(
                "SQL FAIL | %s | %.2fs | %s",
                sql_file.name,
                elapsed,
                e,
            )
            failed.append(sql_file.name)

    return failed
