from pathlib import Path
import duckdb
import logging
import time
import hashlib
from util.run_history import append_run_history
from datetime import datetime

def compute_sql_hash(sql_text: str) -> str:
    """
    SQL 내용 기준 해시
    공백/줄바꿈 영향 줄이려면 strip 정도만 수행
    """
    normalized = sql_text.strip().encode("utf-8")
    return hashlib.md5(normalized).hexdigest()[:10]


def run_duckdb_sql_dir(duckdb_file: Path, sql_dir: Path):

    if not sql_dir.exists():
        logging.info("DuckDB SQL dir not found, skip: %s", sql_dir)
        return

    sql_files = sorted(sql_dir.rglob("*.sql"))

    if not sql_files:
        logging.info("No DuckDB SQL files found in %s", sql_dir)
        return

    total = len(sql_files)

    logging.info(
        "DuckDB postwork start | dir=%s | files=%d",
        sql_dir,
        total,
    )

    con = duckdb.connect(str(duckdb_file))

    try:
        for idx, sql_file in enumerate(sql_files, start=1):
            start = time.time()
            batch_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            sql_text = sql_file.read_text(encoding="utf-8")
            sql_hash = compute_sql_hash(sql_text)

            logging.info(
                "DuckDB SQL [%d/%d] start: %s | hash=%s",
                idx,
                total,
                sql_file.name,
                sql_hash,
            )

            con.execute(sql_text)

            elapsed = round(time.time() - start, 2)

            logging.info(
                "DuckDB SQL [%d/%d] done | %s | %.2fs | hash=%s",
                idx,
                total,
                sql_file.name,
                elapsed,
                sql_hash,
            )

    finally:
        con.close()

    logging.info("DuckDB postwork finished")
