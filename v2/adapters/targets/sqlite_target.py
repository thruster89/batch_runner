# file: v2/adapters/targets/sqlite_target.py

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
            job_name      TEXT,
            table_name    TEXT,
            csv_file      TEXT,
            file_hash     TEXT,
            file_size     INTEGER,
            mtime         TEXT,
            loaded_at     TEXT
        )
        """
    )
    con.commit()


def _history_exists(con, job_name: str, table_name: str, file_hash: str) -> bool:
    cur = con.cursor()
    cur.execute(
        """
        SELECT 1 FROM _LOAD_HISTORY
         WHERE job_name   = ?
           AND table_name = ?
           AND file_hash  = ?
         LIMIT 1
        """,
        (job_name, table_name, file_hash),
    )
    return cur.fetchone() is not None


def _insert_history(con, job_name: str, table_name: str, csv_file: str,
                    file_hash: str, file_size: int, mtime: str):
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO _LOAD_HISTORY
            (job_name, table_name, csv_file, file_hash, file_size, mtime, loaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (job_name, table_name, csv_file, file_hash, file_size, mtime, _now_str()),
    )
    con.commit()


def load_csv(con, job_name: str, table_name: str, csv_path: Path,
             file_hash: str, mode: str) -> int:
    """
    CSV를 SQLite 테이블에 적재.
    반환값: 적재된 row 수 (-1이면 skip)
    """
    import pandas as pd

    file_size = csv_path.stat().st_size
    mtime = datetime.fromtimestamp(csv_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")

    if mode != "retry" and _history_exists(con, job_name, table_name, file_hash):
        logger.info("LOAD skip (already loaded) | %s | %s", table_name, csv_path.name)
        return -1  # skip 표시

    start = time.time()

    df = pd.read_csv(csv_path)
    df.to_sql(table_name, con, if_exists="append", index=False)

    row_count = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

    _insert_history(con, job_name, table_name, str(csv_path), file_hash, file_size, mtime)

    elapsed = time.time() - start
    logger.info("LOAD done | table=%s rows=%d elapsed=%.2fs", table_name, row_count, elapsed)

    return row_count


def connect(db_path: Path):
    import sqlite3
    return sqlite3.connect(str(db_path))