import logging
import duckdb
from pathlib import Path


def create_union_views(
    duckdb_file: Path,
    schema: str,
    tables: set[str],
) -> None:
    """
    suffix parquet/테이블들을 논리 테이블 기준으로 UNION VIEW 생성

    예:
      A1__clsYymm=202312
      A1__clsYymm=202403
      →
      VIEW A1 AS
      SELECT * FROM A1__clsYymm=202312
      UNION ALL
      SELECT * FROM A1__clsYymm=202403
    """

    con = duckdb.connect(duckdb_file.as_posix())

    for table in sorted(tables):
        rows = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name LIKE ?
            ORDER BY table_name
            """,
            [schema, f"{table}__%"],
        ).fetchall()

        if not rows:
            logging.info(
                "UNION VIEW skip (no suffix tables) | %s.%s",
                schema, table,
            )
            continue

        selects = [
            f'SELECT * FROM "{schema}"."{r[0]}"'
            for r in rows
        ]

        union_sql = "\nUNION ALL\n".join(selects)

        view_sql = f'''
        CREATE OR REPLACE VIEW "{schema}"."{table}" AS
        {union_sql}
        '''

        con.execute(view_sql)

        logging.info(
            "UNION VIEW OK | %s.%s | parts=%d",
            schema, table, len(rows),
        )

    con.close()
