import time
import logging
from pathlib import Path
import duckdb

from util.run_history import append_run_history, load_last_success_keys
from util.sql_hash import compute_sql_hash

logger = logging.getLogger(__name__)


def run_duckdb_sql_dir(
    duckdb_file: Path,
    sql_dir: Path,
    batch_ts: str,
    retry: bool = False,
    sql_filter: str | None = None,
):
    """
    DuckDB 내부 SQL 디렉토리 실행

    기능:
      - 파일명 순서대로 실행
      - run_history 기록
      - sql_hash 기반 RETRY skip
      - sql_filter 지원
    """

    if not sql_dir.exists():
        logger.warning("DuckDB SQL dir not found: %s", sql_dir)
        return

    sql_files = sorted(sql_dir.glob("*.sql"))

    # --------------------------------------------
    # SQL FILTER
    # --------------------------------------------
    if sql_filter:
        patterns = [p.strip().lower() for p in sql_filter.split(",")]

        sql_files = [
            f for f in sql_files
            if any(p in f.name.lower() for p in patterns)
        ]

        logger.info("DuckDB SQL filter applied: %s", patterns)

    if not sql_files:
        logger.info("DuckDB SQL: nothing to run")
        return

    success_keys = load_last_success_keys() if retry else set()

    con = duckdb.connect(duckdb_file)

    total = len(sql_files)

    for idx, sql_file in enumerate(sql_files, 1):

        logger.info(
            "DuckDB SQL [%d/%d] start: %s",
            idx,
            total,
            sql_file.name,
        )

        start = time.time()
        sql_hash = "-"
        rel_path_str = sql_file.as_posix()

        try:
            sql_text = sql_file.read_text(encoding="utf-8")
            sql_hash = compute_sql_hash(sql_text)

            key = ("duckdb", rel_path_str, "-", sql_hash)

            # --------------------------------------------
            # RETRY skip
            # --------------------------------------------
            if retry and key in success_keys:
                logger.info(
                    "DuckDB SQL SKIP (already done): %s",
                    sql_file.name,
                )
                continue

            # --------------------------------------------
            # 실행
            # --------------------------------------------
            con.execute(sql_text)

            elapsed = round(time.time() - start, 2)

            logger.info(
                "DuckDB SQL done: %s (%.2fs)",
                sql_file.name,
                elapsed,
            )

            # --------------------------------------------
            # run_history 기록
            # --------------------------------------------
            append_run_history({
                "batch_ts": batch_ts,
                "host": "duckdb",
                "sql_file": rel_path_str,
                "params": "-",
                "sql_hash": sql_hash,
                "status": "OK",
                "rows": "",
                "elapsed_sec": elapsed,
                "output_file": "",
                "error_message": "",
            })

        except Exception as e:
            elapsed = round(time.time() - start, 2)
            error_msg = str(e)[:500]

            logger.error(
                "DuckDB SQL FAIL | %s | %.2fs | %s",
                sql_file.name,
                elapsed,
                error_msg,
            )

            append_run_history({
                "batch_ts": batch_ts,
                "host": "duckdb",
                "sql_file": rel_path_str,
                "params": "-",
                "sql_hash": sql_hash,
                "status": "FAIL",
                "rows": "",
                "elapsed_sec": elapsed,
                "output_file": "",
                "error_message": error_msg,
            })

    con.close()
