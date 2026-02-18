# file: v2/adapters/targets/duckdb_target.py

import time
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ensure_history(con):
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS _LOAD_HISTORY (
            job_name      VARCHAR,
            table_name    VARCHAR,
            csv_file      VARCHAR,
            file_hash     VARCHAR,
            file_size     BIGINT,
            mtime         VARCHAR,
            loaded_at     VARCHAR
        )
        """
    )


def _history_exists(con, job_name: str, table_name: str, file_hash: str) -> bool:
    rows = con.execute(
        """
        SELECT 1 FROM _LOAD_HISTORY
         WHERE job_name  = ?
           AND table_name = ?
           AND file_hash  = ?
         LIMIT 1
        """,
        [job_name, table_name, file_hash],
    ).fetchall()
    return bool(rows)


def _insert_history(con, job_name: str, table_name: str, csv_file: str,
                    file_hash: str, file_size: int, mtime: str):
    con.execute(
        """
        INSERT INTO _LOAD_HISTORY
            (job_name, table_name, csv_file, file_hash, file_size, mtime, loaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [job_name, table_name, csv_file, file_hash, file_size, mtime, _now_str()],
    )


def _table_exists(con, table_name: str) -> bool:
    rows = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1",
        [table_name],
    ).fetchall()
    return bool(rows)


def load_csv(con, job_name: str, table_name: str, csv_path: Path,
             file_hash: str, mode: str) -> int:
    """
    CSV를 DuckDB 테이블에 적재.
    반환값: 적재된 row 수
    """
    file_size = csv_path.stat().st_size
    mtime = datetime.fromtimestamp(csv_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")

    if mode != "retry" and _history_exists(con, job_name, table_name, file_hash):
        logger.info("LOAD skip (already loaded) | %s | %s", table_name, csv_path.name)
        return -1  # skip 표시

    start = time.time()

    if not _table_exists(con, table_name):
        con.execute(
            f'CREATE TABLE "{table_name}" AS SELECT * FROM read_csv_auto(?, header=True)',
            [str(csv_path)],
        )
    else:
        con.execute(
            f'INSERT INTO "{table_name}" SELECT * FROM read_csv_auto(?, header=True)',
            [str(csv_path)],
        )

    row_count = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

    _insert_history(con, job_name, table_name, str(csv_path), file_hash, file_size, mtime)

    elapsed = time.time() - start
    logger.info("LOAD done | table=%s rows=%d elapsed=%.2fs", table_name, row_count, elapsed)

    return row_count


def connect(db_path: Path):
    import duckdb
    return duckdb.connect(str(db_path))