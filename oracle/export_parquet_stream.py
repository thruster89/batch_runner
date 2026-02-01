import time
from itertools import product

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from util.paths import SQL_DIR, PARQUET_DIR
from util.logging import get_host_logger
from util.filename_suffix import build_param_suffix
from util.param_expand import expand_param_value

from oracle.client import get_oracle_conn
from oracle.sql_utils import normalize_sql, extract_params, apply_params

CHUNK_SIZE = 1_000_000


def export_oracle_to_parquet_stream(
    host_name: str,
    host_cfg: dict,
    sql_files: list,
    params: dict,
    batch_date: str,
) -> list[str]:
    failed: list[str] = []

    schema = host_cfg["duckdb_schema"]
    host_logger = get_host_logger(host_name, batch_date)

    base_out = PARQUET_DIR / schema

    for sql_file in sql_files:
        start_sql = time.time()
        writer = None

        try:
            rel = sql_file.relative_to(SQL_DIR / schema)
            subdir = rel.parent
            table = rel.stem

            out_dir = base_out / subdir
            out_dir.mkdir(parents=True, exist_ok=True)

            sql_raw = normalize_sql(sql_file.read_text(encoding="utf-8"))
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
                out_file.parent.mkdir(parents=True, exist_ok=True)

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
                        "PARQUET EMPTY | %s | rows=0",
                        f"{rel.as_posix()}{suffix}",
                    )
                else:
                    size_mb = out_file.stat().st_size / (1024 * 1024)
                    host_logger.info(
                        "PARQUET OK | %s | rows=%d | %.2fs | %.2fMB",
                        f"{rel.as_posix()}{suffix}",
                        total_rows,
                        elapsed,
                        size_mb,
                    )

        except Exception as e:
            host_logger.error(
                "PARQUET FAIL | %s | %s",
                sql_file.name,
                e,
            )
            failed.append(sql_file.name)

        finally:
            if writer:
                writer.close()

    return failed
