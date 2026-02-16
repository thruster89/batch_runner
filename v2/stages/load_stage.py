# file: v2/stages/load_stage.py

import hashlib
import time
from datetime import datetime
from pathlib import Path

from v2.engine.path_utils import resolve_path
from v2.engine.sql_utils import sort_sql_files, resolve_table_name, extract_sqlname_from_csv


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _ensure_duckdb_history(con):
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


def _ensure_sqlite_history(con):
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


def _history_exists_duckdb(con, job_name: str, table_name: str, file_hash: str) -> bool:
    rows = con.execute(
        """
        SELECT 1
          FROM _LOAD_HISTORY
         WHERE job_name = ?
           AND table_name = ?
           AND file_hash = ?
         LIMIT 1
        """,
        [job_name, table_name, file_hash],
    ).fetchall()
    return bool(rows)


def _history_exists_sqlite(con, job_name: str, table_name: str, file_hash: str) -> bool:
    cur = con.cursor()
    cur.execute(
        """
        SELECT 1
          FROM _LOAD_HISTORY
         WHERE job_name = ?
           AND table_name = ?
           AND file_hash = ?
         LIMIT 1
        """,
        (job_name, table_name, file_hash),
    )
    return cur.fetchone() is not None


def _insert_history_duckdb(con, job_name: str, table_name: str, csv_file: str, file_hash: str, file_size: int, mtime: str):
    con.execute(
        """
        INSERT INTO _LOAD_HISTORY(job_name, table_name, csv_file, file_hash, file_size, mtime, loaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [job_name, table_name, csv_file, file_hash, file_size, mtime, _now_str()],
    )


def _insert_history_sqlite(con, job_name: str, table_name: str, csv_file: str, file_hash: str, file_size: int, mtime: str):
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO _LOAD_HISTORY(job_name, table_name, csv_file, file_hash, file_size, mtime, loaded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (job_name, table_name, csv_file, file_hash, file_size, mtime, _now_str()),
    )
    con.commit()


def run(ctx):
    logger = ctx.logger
    job_cfg = ctx.job_config

    logger.info("LOAD stage start")

    # PLAN/RETRY 정책은 아직 확정된 코드가 없어서, 최소 동작만:
    if ctx.mode == "plan":
        logger.info("LOAD stage skipped (plan mode)")
        logger.info("LOAD stage end")
        return

    export_cfg = job_cfg.get("export", {})
    if not export_cfg:
        logger.info("LOAD stage skipped (no export config)")
        logger.info("LOAD stage end")
        return

    target_cfg = job_cfg.get("target", {})
    if not target_cfg:
        logger.info("LOAD stage skipped (no target config)")
        logger.info("LOAD stage end")
        return

    # export 결과 폴더: out_dir/job_name 우선, 없으면 out_dir 그대로 fallback
    export_base = resolve_path(ctx, export_cfg.get("out_dir", "data/export"))
    export_dir = export_base / ctx.job_name
    if not export_dir.exists():
        export_dir = export_base

    # backup 폴더 제외
    csv_files = sorted([p for p in export_dir.glob("*.csv") if p.is_file()])
    if not csv_files:
        logger.warning("No CSV files found in %s", export_dir)
        logger.info("LOAD stage end")
        return

    # SQL -> table_name 매핑 준비
    sql_dir = resolve_path(ctx, export_cfg.get("sql_dir", "sql/export"))
    sql_files = sort_sql_files(sql_dir)
    sql_map = {p.stem: p for p in sql_files}  # stem 기준

    tgt_type = (target_cfg.get("type") or "").strip().lower()

    if tgt_type == "duckdb":
        import duckdb

        db_path = resolve_path(ctx, target_cfg.get("db_path", "data/local/result.duckdb"))
        con = duckdb.connect(str(db_path))
        _ensure_duckdb_history(con)

        try:
            for i, csv_path in enumerate(csv_files, 1):
                sqlname = extract_sqlname_from_csv(csv_path)
                sql_file = sql_map.get(sqlname)

                if not sql_file:
                    logger.warning("CSV[%d/%d] skip (sql not found): %s", i, len(csv_files), csv_path.name)
                    continue

                table_name = resolve_table_name(sql_file)

                file_hash = _sha256_file(csv_path)
                file_size = csv_path.stat().st_size
                mtime = datetime.fromtimestamp(csv_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")

                if _history_exists_duckdb(con, ctx.job_name, table_name, file_hash):
                    logger.info("LOAD skip (already loaded) | %s | %s", table_name, csv_path.name)
                    continue

                logger.info("LOAD start [%d/%d] | table=%s | file=%s", i, len(csv_files), table_name, csv_path.name)

                start = time.time()

                # DuckDB: CSV를 table로 적재 (단순 insert)
                # - 테이블이 없으면 자동 생성 필요: DuckDB는 CREATE TABLE AS SELECT 지원
                con.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS "{table_name}" AS
                    SELECT * FROM read_csv_auto(?, header=True)
                    """,
                    [str(csv_path)],
                )

                # 테이블이 이미 있으면 append
                # (위 CTAS는 이미 존재 시 실행되지 않으므로, append를 별도로)
                con.execute(
                    f"""
                    INSERT INTO "{table_name}"
                    SELECT * FROM read_csv_auto(?, header=True)
                    """,
                    [str(csv_path)],
                )

                _insert_history_duckdb(
                    con,
                    job_name=ctx.job_name,
                    table_name=table_name,
                    csv_file=str(csv_path),
                    file_hash=file_hash,
                    file_size=file_size,
                    mtime=mtime,
                )

                elapsed = time.time() - start
                logger.info("LOAD done [%d/%d] | table=%s elapsed=%.2fs", i, len(csv_files), table_name, elapsed)

        finally:
            con.close()

    elif tgt_type == "sqlite3":
        import sqlite3

        db_path = resolve_path(ctx, target_cfg.get("db_path", "data/local/result.sqlite"))
        con = sqlite3.connect(str(db_path))
        _ensure_sqlite_history(con)

        try:
            # SQLite는 read_csv_auto 같은 함수가 없어서, 최소 구현으로 pandas 사용
            import pandas as pd

            for i, csv_path in enumerate(csv_files, 1):
                sqlname = extract_sqlname_from_csv(csv_path)
                sql_file = sql_map.get(sqlname)

                if not sql_file:
                    logger.warning("CSV[%d/%d] skip (sql not found): %s", i, len(csv_files), csv_path.name)
                    continue

                table_name = resolve_table_name(sql_file)

                file_hash = _sha256_file(csv_path)
                file_size = csv_path.stat().st_size
                mtime = datetime.fromtimestamp(csv_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")

                if _history_exists_sqlite(con, ctx.job_name, table_name, file_hash):
                    logger.info("LOAD skip (already loaded) | %s | %s", table_name, csv_path.name)
                    continue

                logger.info("LOAD start [%d/%d] | table=%s | file=%s", i, len(csv_files), table_name, csv_path.name)

                start = time.time()

                df = pd.read_csv(csv_path)
                df.to_sql(table_name, con, if_exists="append", index=False)

                _insert_history_sqlite(
                    con,
                    job_name=ctx.job_name,
                    table_name=table_name,
                    csv_file=str(csv_path),
                    file_hash=file_hash,
                    file_size=file_size,
                    mtime=mtime,
                )

                elapsed = time.time() - start
                logger.info("LOAD done [%d/%d] | table=%s elapsed=%.2fs", i, len(csv_files), table_name, elapsed)

        finally:
            con.close()

    elif tgt_type == "oracle":
            import csv
            from v2.adapters.sources.oracle_client import get_oracle_conn

            oracle_cfg = ctx.env_config["sources"]["oracle"]

            # target은 항상 local
            host_cfg = oracle_cfg["hosts"].get("local")
            if not host_cfg:
                raise RuntimeError("Oracle target requires hosts.local in env.yml")

            conn = get_oracle_conn(host_cfg)
            cur = conn.cursor()

            try:
                for i, csv_path in enumerate(csv_files, 1):

                    sqlname = extract_sqlname_from_csv(csv_path)
                    sql_file = sql_map.get(sqlname)

                    if not sql_file:
                        logger.warning("CSV[%d/%d] skip (sql not found): %s", i, len(csv_files), csv_path.name)
                        continue

                    table_name = resolve_table_name(sql_file)

                    file_hash = _sha256_file(csv_path)
                    file_size = csv_path.stat().st_size
                    mtime = datetime.fromtimestamp(csv_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")

                    # load_history 테이블 생성
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

                    cur.execute("""
                        SELECT COUNT(1)
                        FROM _LOAD_HISTORY
                        WHERE job_name = :1
                        AND table_name = :2
                        AND file_hash = :3
                    """, (ctx.job_name, table_name, file_hash))

                    if cur.fetchone()[0] > 0:
                        logger.info("LOAD skip (already loaded) | %s | %s", table_name, csv_path.name)
                        continue

                    logger.info("LOAD start [%d/%d] | table=%s | file=%s", i, len(csv_files), table_name, csv_path.name)

                    start = time.time()

                    # CSV insert
                    with open(csv_path, "r", encoding="utf-8") as f:
                        reader = csv.reader(f)
                        headers = next(reader)

                        placeholders = ",".join([f":{i+1}" for i in range(len(headers))])
                        insert_sql = f'INSERT INTO {table_name} VALUES ({placeholders})'

                        batch = []
                        batch_size = 1000

                        for row in reader:
                            batch.append(row)
                            if len(batch) >= batch_size:
                                cur.executemany(insert_sql, batch)
                                batch.clear()

                        if batch:
                            cur.executemany(insert_sql, batch)

                    cur.execute("""
                        INSERT INTO _LOAD_HISTORY
                        (job_name, table_name, csv_file, file_hash, file_size, mtime, loaded_at)
                        VALUES (:1,:2,:3,:4,:5,:6,:7)
                    """, (
                        ctx.job_name,
                        table_name,
                        str(csv_path),
                        file_hash,
                        file_size,
                        mtime,
                        _now_str()
                    ))

                    conn.commit()

                    elapsed = time.time() - start
                    logger.info("LOAD done [%d/%d] | table=%s elapsed=%.2fs", i, len(csv_files), table_name, elapsed)

            finally:
                cur.close()
                conn.close()        

    else:
        raise ValueError(f"Unsupported target type: {tgt_type}")

    logger.info("LOAD stage end")
