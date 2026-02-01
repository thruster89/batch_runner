import duckdb
from util.paths import DEFAULT_DUCKDB_FILE

def create_union_views(host_schemas):
    con = duckdb.connect(DEFAULT_DUCKDB_FILE)
    con.execute("CREATE SCHEMA IF NOT EXISTS UNION_ALL")

    tables = {}
    for schema in host_schemas:
        rows = con.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
        """).fetchall()
        for (t,) in rows:
            tables.setdefault(t, []).append(schema)

    for table, schemas in tables.items():
        if len(schemas) < 2:
            continue

        union_sql = " UNION ALL ".join(
            f"SELECT *, '{s}' AS src_schema FROM {s}.{table}"
            for s in schemas
        )

        con.execute(f"""
            CREATE OR REPLACE VIEW UNION_ALL.{table} AS
            {union_sql}
        """)

    con.close()
