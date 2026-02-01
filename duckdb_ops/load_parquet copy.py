import logging
import duckdb
from pathlib import Path
from util.paths import PARQUET_DIR
from util.filename_suffix import split_table_and_suffix


def _ensure_load_history(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS _LOAD_HISTORY (
            schema_name  VARCHAR,
            table_name   VARCHAR,
            file_suffix  VARCHAR,
            file_path    VARCHAR,
            loaded_at    TIMESTAMP DEFAULT now()
        );
        """
    )


def _already_loaded(con, schema: str, table: str, suffix: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM _LOAD_HISTORY
        WHERE schema_name = ?
          AND table_name  = ?
          AND file_suffix = ?
        LIMIT 1
        """,
        [schema, table, suffix],
    ).fetchone()
    return row is not None


def _mark_loaded(con, schema: str, table: str, suffix: str, file_path: str) -> int:
    con.execute(
        """
        INSERT INTO _LOAD_HISTORY(schema_name, table_name, file_suffix, file_path)
        VALUES (?, ?, ?, ?)
        """,
        [schema, table, suffix, file_path],
    )
    return con.execute(
        """
        SELECT COUNT(*)
        FROM _LOAD_HISTORY
        WHERE schema_name = ?
          AND table_name  = ?
        """,
        [schema, table],
    ).fetchone()[0]


def load_parquet_to_duckdb(duckdb_file: Path, schema: str, target_tables: set[str]) -> None:
    duckdb_file.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(duckdb_file)

    schema_q = f'"{schema}"'
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_q}")

    _ensure_load_history(con)

    base = PARQUET_DIR / schema
    if not base.exists():
        logging.warning("Parquet folder not found, skip load | %s", base.as_posix())
        con.close()
        return

    for pq_file in base.rglob("*.parquet"):
        rel = pq_file.relative_to(base)

        stem = pq_file.stem
        table_base, suffix = split_table_and_suffix(stem)

        # 이번 실행 대상만
        if table_base not in target_tables:
            continue

        table_q = f'"{table_base}"'
        full_table = f"{schema_q}.{table_q}"

        # =============================
        # 1️⃣ suffix 없는 기준 테이블
        # =============================
        if not suffix:
            exists = con.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = ?
                  AND table_name   = ?
                """,
                [schema, table_base],
            ).fetchone()

            if exists:
                logging.info(
                    "DuckDB load SKIP (base table, no suffix) | %s.%s",
                    schema,
                    table_base,
                )
                continue

            con.execute(
                f"""
                CREATE TABLE {full_table} AS
                SELECT * FROM read_parquet('{pq_file.as_posix()}')
                """
            )

            rows = con.execute(
                f"SELECT COUNT(*) FROM {full_table}"
            ).fetchone()[0]

            logging.info(
                "DuckDB CREATE OK (base table) | %s.%s | rows=%d | file=%s",
                schema,
                table_base,
                rows,
                rel.as_posix(),
            )
            continue

        # =============================
        # 2️⃣ suffix 있는 팩트 테이블
        # =============================
        if _already_loaded(con, schema, table_base, suffix):
            logging.info(
                "DuckDB load SKIP (already loaded) | %s.%s%s",
                schema,
                table_base,
                suffix,
            )
            continue

        exists = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name   = ?
            """,
            [schema, table_base],
        ).fetchone()

        if not exists:
            con.execute(
                f"""
                CREATE TABLE {full_table} AS
                SELECT * FROM read_parquet('{pq_file.as_posix()}')
                """
            )
            action = "CREATE"
        else:
            con.execute(
                f"""
                INSERT INTO {full_table}
                SELECT * FROM read_parquet('{pq_file.as_posix()}')
                """
            )
            action = "INSERT"

        rows = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{pq_file.as_posix()}')"
        ).fetchone()[0]

        hist_cnt = _mark_loaded(con, schema, table_base, suffix, pq_file.as_posix())

        logging.info(
            "DuckDB %s OK (parquet) | %s.%s%s | rows=%d | history=%d | file=%s",
            action,
            schema,
            table_base,
            suffix,
            rows,
            hist_cnt,
            rel.as_posix(),
        )

    con.close()
