# duckdb_ops/load_csv.py

import logging
import duckdb
from pathlib import Path

from util.paths import CSV_DIR


def _split_table_and_suffix(filename: str) -> tuple[str, str]:
    """
    파일명에서 (TABLE, SUFFIX) 분리

    기대 파일명:
      - rate.csv.gz                       -> ("RATE", "")
      - a1__clsYymm=202312.csv.gz         -> ("A1", "__clsYymm=202312")
      - a2__clsYymm=202403_prodcd=K1.csv.gz -> ("A2", "__clsYymm=202403_prodcd=K1")

    주의:
      - .csv.gz / .gz / .csv 확장자를 확실히 제거
      - suffix는 "__..."만 유지
    """
    name = filename

    # 확장자 제거 (뒤에서부터 안전하게)
    if name.lower().endswith(".csv.gz"):
        name = name[:-7]  # remove ".csv.gz"
    elif name.lower().endswith(".gz"):
        name = name[:-3]  # remove ".gz"
        if name.lower().endswith(".csv"):
            name = name[:-4]  # remove ".csv"
    else:
        if name.lower().endswith(".csv"):
            name = name[:-4]

    # suffix 분리
    if "__" in name:
        base, rest = name.split("__", 1)
        table = base.upper()
        suffix = "__" + rest
    else:
        table = name.upper()
        suffix = ""

    return table, suffix


def load_csv_to_duckdb(duckdb_file: Path, schema: str, target_tables: set[str]) -> None:
    """
    CSV → DuckDB 적재

    - 파일명 suffix("__k=v...")가 있으면:
        * base 테이블 없으면 CREATE
        * base 테이블 있으면 INSERT
        * (schema, table, suffix) 가 _LOAD_HISTORY에 있으면 SKIP

    - suffix가 없으면(rate.csv.gz 같은):
        * suffix=""로 간주
        * (schema, table, "") 가 _LOAD_HISTORY에 있으면 SKIP
        * 없으면 CREATE/INSERT 후 history 기록

    ✅ 핵심: rate.csv.gz를 반드시 table="RATE"로 파싱해야 target_tables에 걸림
    """

    duckdb_file.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(duckdb_file)

    # schema 생성
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    # history 테이블 생성
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

    base_dir = CSV_DIR / schema

    inserted_hist = 0
    skipped_hist = 0
    created_tbl = 0
    inserted_tbl = 0

    for csv_file in base_dir.rglob("*.csv.gz"):
        table, suffix = _split_table_and_suffix(csv_file.name)

        # 이번 실행 대상 테이블만
        if table not in target_tables:
            # B/rate가 여기서 걸리면 문제였던 거임 (이제 안 걸려야 정상)
            continue

        # history 중복 체크
        already = con.execute(
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

        if already:
            logging.info(
                "DuckDB load SKIP (already loaded) | %s.%s%s",
                schema,
                table,
                suffix,
            )
            skipped_hist += 1
            continue

        table_q = f'"{schema}"."{table}"'

        # 테이블 존재 여부
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

        # CREATE or INSERT
        if not table_exists:
            con.execute(
                f"""
                CREATE TABLE {table_q} AS
                SELECT * FROM read_csv_auto('{csv_file.as_posix()}')
                """
            )
            row_count = con.execute(
                f"SELECT COUNT(*) FROM {table_q}"
            ).fetchone()[0]

            logging.info(
                "DuckDB CREATE OK | %s.%s%s | rows=%d",
                schema,
                table,
                suffix,
                row_count,
            )
            created_tbl += 1
        else:
            con.execute(
                f"""
                INSERT INTO {table_q}
                SELECT * FROM read_csv_auto('{csv_file.as_posix()}')
                """
            )
            row_count = con.execute(
                f"SELECT COUNT(*) FROM read_csv_auto('{csv_file.as_posix()}')"
            ).fetchone()[0]

            logging.info(
                "DuckDB INSERT OK | %s.%s%s | rows=%d",
                schema,
                table,
                suffix,
                row_count,
            )
            inserted_tbl += 1

        # history 기록
        con.execute(
            """
            INSERT INTO _LOAD_HISTORY
            (schema_name, table_name, file_suffix, loaded_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [schema, table, suffix],
        )
        inserted_hist += 1

    con.close()

    logging.info(
        "DuckDB load summary | schema=%s | created=%d | inserted=%d | hist_inserted=%d | hist_skipped=%d",
        schema,
        created_tbl,
        inserted_tbl,
        inserted_hist,
        skipped_hist,
    )
