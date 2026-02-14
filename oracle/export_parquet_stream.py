import time
from itertools import product

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from util.paths import SQL_DIR, PARQUET_DIR
from util.logging import get_host_logger
from util.filename_suffix import build_param_suffix
from util.param_expand import expand_param_value
from util.run_history import append_run_history, load_last_success_keys

from oracle.client import get_oracle_conn
from oracle.sql_utils import normalize_sql, extract_params, apply_params
from util.sql_hash import compute_sql_hash

CHUNK_SIZE = 1_000_000


def export_oracle_to_parquet_stream(
    source,              # ★ 추가
    host_name,
    host_cfg,
    sql_files,
    params,
    batch_ts,            # ★ batch_ts로 통일
    retry=False,
):

    success_keys = load_last_success_keys() if retry else set()
    failed: list[str] = []

    schema = host_cfg.get("duckdb_schema", host_name)
    host_logger = get_host_logger(host_name, batch_ts)

    base_out = PARQUET_DIR / schema

    for sql_file in sql_files:
        start_sql = time.time()
        writer = None
        param_desc = "-"
        rel_path_str = sql_file.name
        sql_hash = "-"

        try:
            rel = sql_file.relative_to(SQL_DIR / source / host_name)
            rel_path_str = rel.as_posix()

            subdir = rel.parent
            table = rel.stem

            out_dir = base_out / subdir
            out_dir.mkdir(parents=True, exist_ok=True)

            sql_raw = normalize_sql(sql_file.read_text(encoding="utf-8"))
            sql_hash = compute_sql_hash(sql_raw)
            used_keys = sorted(extract_params(sql_raw))

            expand_values = [
                expand_param_value(str(params[k]))
                for k in used_keys
            ] if used_keys else [[]]

            param_cases = (
                [dict(zip(used_keys, combo)) for combo in product(*expand_values)]
                if expand_values != [[]]
                else [{}]
            )

            for case_params in param_cases:
                full_params = params.copy()
                full_params.update(case_params)

                suffix = build_param_suffix(full_params, used_keys)
                out_file = out_dir / f"{table}{suffix}.parquet"

                param_desc = ", ".join(
                    f"{k}={full_params[k]}" for k in sorted(used_keys)
                ) or "-"

                key = (host_name, rel_path_str, param_desc, sql_hash)

                if retry and key in success_keys and out_file.exists():
                    host_logger.info(
                        "PARQUET SKIP (already done) | %s | %s",
                        rel_path_str,
                        param_desc,
                    )
                    continue

                if out_file.exists():
                    host_logger.info(
                        "Parquet exists, skip export | %s",
                        out_file.as_posix(),
                    )
                    continue

                sql = apply_params(sql_raw, full_params)

                total_rows = 0
                writer = None

                with get_oracle_conn(host_cfg) as conn:
                    for chunk in pd.read_sql(sql, conn, chunksize=CHUNK_SIZE):
                        if chunk.empty:
                            continue

                        total_rows += len(chunk)
                        arrow = pa.Table.from_pandas(chunk, preserve_index=False)

                        if writer is None:
                            writer = pq.ParquetWriter(
                                out_file,
                                arrow.schema,
                                compression="snappy",
                            )

                        writer.write_table(arrow)

                if writer:
                    writer.close()
                    writer = None

                elapsed = round(time.time() - start_sql, 2)

                if total_rows == 0:
                    host_logger.warning(
                        "PARQUET EMPTY | %s | %s | rows=0",
                        rel_path_str,
                        param_desc,
                    )
                else:
                    size_mb = out_file.stat().st_size / (1024 * 1024)
                    host_logger.info(
                        "PARQUET OK | %s | %s | rows=%d | %.2fMB | %.2fs",
                        rel_path_str,
                        param_desc,
                        total_rows,
                        size_mb,
                        elapsed,
                    )

                append_run_history({
                    "batch_ts": batch_ts,
                    "host": host_name,
                    "sql_file": rel_path_str,
                    "params": param_desc,
                    "sql_hash": sql_hash,
                    "status": "OK",
                    "rows": total_rows,
                    "elapsed_sec": elapsed,
                    "output_file": out_file.as_posix(),
                    "error_message": "",
                })

        except Exception as e:
            elapsed = round(time.time() - start_sql, 2)
            error_msg = str(e)[:500]

            host_logger.error(
                "PARQUET FAIL | %s | %.2fs | %s",
                rel_path_str,
                elapsed,
                error_msg,
            )

            failed.append(rel_path_str)

            append_run_history({
                "batch_ts": batch_ts,
                "host": host_name,
                "sql_file": rel_path_str,
                "params": param_desc,
                "sql_hash": sql_hash,
                "status": "FAIL",
                "rows": 0,
                "elapsed_sec": elapsed,
                "output_file": "",
                "error_message": error_msg,
            })

        finally:
            if writer:
                writer.close()

    return failed
