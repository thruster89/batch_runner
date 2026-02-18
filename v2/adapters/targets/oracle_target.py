# file: v2/adapters/targets/oracle_target.py

import csv
import gzip
import time
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ensure_history(cur):
    cur.execute("""
        BEGIN
            EXECUTE IMMEDIATE '
                CREATE TABLE _LOAD_HISTORY (
                    job_name   VARCHAR2(100),
                    table_name VARCHAR2(100),
                    csv_file   VARCHAR2(500),
                    file_hash  VARCHAR2(64),
                    file_size  NUMBER,
                    mtime      VARCHAR2(30),
                    loaded_at  VARCHAR2(30)
                )';
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -955 THEN RAISE; END IF;
        END;
    """)


def _history_exists(cur, job_name: str, table_name: str, file_hash: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(1) FROM _LOAD_HISTORY
         WHERE job_name   = :1
           AND table_name = :2
           AND file_hash  = :3
        """,
        (job_name, table_name, file_hash),
    )
    return cur.fetchone()[0] > 0


def _insert_history(cur, conn, job_name: str, table_name: str, csv_file: str,
                    file_hash: str, file_size: int, mtime: str):
    cur.execute(
        """
        INSERT INTO _LOAD_HISTORY
            (job_name, table_name, csv_file, file_hash, file_size, mtime, loaded_at)
        VALUES (:1, :2, :3, :4, :5, :6, :7)
        """,
        (job_name, table_name, csv_file, file_hash, file_size, mtime, _now_str()),
    )
    conn.commit()


def load_csv(conn, job_name: str, table_name: str, csv_path: Path,
             file_hash: str, mode: str) -> int:
    """
    CSV를 Oracle 테이블에 적재.
    반환값: 적재된 row 수 (-1이면 skip)
    """
    cur = conn.cursor()
    file_size = csv_path.stat().st_size
    mtime = datetime.fromtimestamp(csv_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")

    try:
        _ensure_history(cur)

        if mode != "retry" and _history_exists(cur, job_name, table_name, file_hash):
            logger.info("LOAD skip (already loaded) | %s | %s", table_name, csv_path.name)
            return -1  # skip 표시

        start = time.time()

        total_rows = 0
        open_fn = gzip.open if str(csv_path).endswith(".gz") else open
        with open_fn(csv_path, "rt", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)

            placeholders = ",".join([f":{j + 1}" for j in range(len(headers))])
            insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

            batch = []
            batch_size = 1000

            for row in reader:
                batch.append(row)
                total_rows += 1
                if len(batch) >= batch_size:
                    cur.executemany(insert_sql, batch)
                    batch.clear()

            if batch:
                cur.executemany(insert_sql, batch)

        _insert_history(cur, conn, job_name, table_name, str(csv_path),
                        file_hash, file_size, mtime)

        elapsed = time.time() - start
        logger.info("LOAD done | table=%s rows=%d elapsed=%.2fs", table_name, total_rows, elapsed)

        return total_rows

    finally:
        cur.close()


def connect(env_config: dict):
    """
    env_config: env.yml 전체 dict
    target은 항상 local Oracle로 연결
    """
    from v2.adapters.sources.oracle_client import init_oracle_client, get_oracle_conn

    oracle_cfg = env_config.get("sources", {}).get("oracle", {})
    if not oracle_cfg:
        raise RuntimeError("oracle config not found in env_config['sources']['oracle']")

    host_cfg = oracle_cfg.get("hosts", {}).get("local")
    if not host_cfg:
        raise RuntimeError("Oracle target requires hosts.local in env.yml")

    init_oracle_client(oracle_cfg)
    return get_oracle_conn(host_cfg)