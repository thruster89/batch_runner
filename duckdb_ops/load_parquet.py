import logging
import duckdb
from pathlib import Path

from util.filename_suffix import split_table_and_suffix


def load_parquet_to_duckdb(
    duckdb_file: Path,
    schema: str,
    target_tables: set[str],
    params: dict | None = None,
) -> None:
    """
    Parquet → DuckDB 적재

    규칙:
    - suffix parquet만 있어도 최초 1회 CREATE
    - 이후 suffix parquet은 INSERT
    - 이미 로드한 suffix는 _LOAD_HISTORY 기준 SKIP
    - params 지정 시 suffix 기준 필터링
    """

    duckdb_file.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(duckdb_file.as_posix())

    # -------------------------------------------------
    # schema & load history
    # -------------------------------------------------
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS _LOAD_HISTORY (
            schema_name  VARCHAR,
            table_name   VARCHAR,
            file_suffix  VARCHAR,
            loaded_at    TIMESTAMP
        )
        """
    )

    base_dir = Path("data/parquet") / schema

    for pq_file in sorted(base_dir.rglob("*.parquet")):
        table, suffix = split_table_and_suffix(pq_file.stem)
        table = table.upper()

        # 이번 실행 대상 테이블만
        if table not in target_tables:
            continue

        # -------------------------------------------------
        # params 필터 (A안)
        # -------------------------------------------------
        if params and suffix:
            matched = True
            for k, v in params.items():
                if f"{k}={v}" not in suffix:
                    matched = False
                    break
            if not matched:
                continue

        # -------------------------------------------------
        # load history 체크
        # -------------------------------------------------
        loaded = con.execute(
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

        if loaded:
            logging.info(
                "DuckDB load SKIP (already loaded) | %s.%s%s",
                schema, table, suffix,
            )
            continue

        table_q = f'"{schema}"."{table}"'

        table_exists = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name   = ?
            LIMIT 1
            """,
            [schema, table],
        ).fetchone()

        # -------------------------------------------------
        # CREATE or INSERT
        # -------------------------------------------------
        if not table_exists:
            con.execute(
                f"""
                CREATE TABLE {table_q} AS
                SELECT * FROM read_parquet('{pq_file.as_posix()}')
                """
            )
            rows = con.execute(
                f"SELECT COUNT(*) FROM {table_q}"
            ).fetchone()[0]

            logging.info(
                "DuckDB CREATE OK | %s.%s | rows=%d",
                schema, table, rows,
            )
        else:
            con.execute(
                f"""
                INSERT INTO {table_q}
                SELECT * FROM read_parquet('{pq_file.as_posix()}')
                """
            )
            rows = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{pq_file.as_posix()}')"
            ).fetchone()[0]

            logging.info(
                "DuckDB INSERT OK | %s.%s%s | rows=%d",
                schema, table, suffix, rows,
            )

        # -------------------------------------------------
        # load history 기록
        # -------------------------------------------------
        con.execute(
            """
            INSERT INTO _LOAD_HISTORY
            (schema_name, table_name, file_suffix, loaded_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [schema, table, suffix],
        )

    con.close()
