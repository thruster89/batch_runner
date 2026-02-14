import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from util.paths import SQL_DIR, PARQUET_DIR
from util.logging import get_host_logger
from util.param_expand import expand_param_value
from util.filename_suffix import build_param_suffix

from oracle.client import get_oracle_conn
from oracle.sql_utils import normalize_sql, extract_params, apply_params

CHUNK_SIZE = 1_000_000


def export_oracle_to_parquet(host_name, host_cfg, sql_files, params, batch_date):
    """
    Oracle → Parquet export

    기준:
      - SQL 위치: host_name 기준
      - Parquet 출력: host_name 기준
      - DuckDB schema 개념 없음
    """

    failed = []
    host_logger = get_host_logger(host_name, batch_date)

    base_out = PARQUET_DIR / host_name

    for sql_file in sql_files:
        sql_start = time.time()

        try:
            # SQL 파일 경로 해석
            rel = sql_file.relative_to(SQL_DIR / host_name)
            subdir = rel.parent
            table_name = rel.stem

            # 출력 디렉토리
            out_dir = base_out / subdir
            out_dir.mkdir(parents=True, exist_ok=True)

            # SQL 로드 및 파라미터 분석
            sql_raw = normalize_sql(sql_file.read_text(encoding="utf-8"))
            used_keys = extract_params(sql_raw)

            expand_keys = sorted(used_keys)
            expand_values = (
                [expand_param_value(str(params[k])) for k in expand_keys]
                if expand_keys else [[]]
            )

            cases = zip(*expand_values) if expand_values != [[]] else [()]

            # 파라미터 케이스 반복
            for values in cases:
                case_params = params.copy()
                for k, v in zip(expand_keys, values):
                    case_params[k] = v

                suffix = build_param_suffix(case_params, expand_keys)
                out_file = out_dir / f"{table_name}{suffix}.parquet"

                if out_file.exists():
                    host_logger.info(
                        "Parquet exists, skip export | %s",
                        out_file.as_posix(),
                    )
                    continue

                sql = apply_params(sql_raw, case_params)

                # Oracle fetch
                with get_oracle_conn(host_cfg) as conn:
                    chunks = list(
                        pd.read_sql(sql, conn, chunksize=CHUNK_SIZE)
                    )

                if not chunks:
                    host_logger.warning(
                        "PARQUET EMPTY | %s | %s",
                        rel.as_posix(),
                        ", ".join(f"{k}={case_params[k]}" for k in expand_keys) or "-",
                    )
                    continue

                df = pd.concat(chunks, ignore_index=True)

                pq.write_table(
                    pa.Table.from_pandas(df, preserve_index=False),
                    out_file,
                )

                elapsed = round(time.time() - sql_start, 2)
                size_mb = out_file.stat().st_size / (1024 * 1024)

                host_logger.info(
                    "PARQUET OK | %s | rows=%d | %.2fs | %.2fMB",
                    f"{rel.as_posix()}{suffix}",
                    len(df),
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

    return failed
