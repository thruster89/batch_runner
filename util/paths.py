# util/paths.py
from pathlib import Path

# =========================================================
# BASE
# =========================================================
BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = BASE_DIR / "data"
CSV_DIR = DATA_DIR / "csv"
PARQUET_DIR = DATA_DIR / "parquet"
EXCEL_DIR = DATA_DIR / "excel"

SQL_DIR = BASE_DIR / "sql"
FAILED_DIR = BASE_DIR / "failed"

LOG_DIR = BASE_DIR / "logs"
DUCKDB_DIR = BASE_DIR / "duckdb"


def resolve_duckdb_file(duckdb_file_arg: str | None) -> Path:
    """
    --duckdb-file 처리 규칙
    - 파일명만 주면: BASE_DIR/duckdb/<name>.duckdb
    - 경로 주면: 그대로 사용
    - 미지정 시: BASE_DIR/duckdb/batch.duckdb
    """
    duckdb_dir = BASE_DIR / "duckdb"
    duckdb_dir.mkdir(parents=True, exist_ok=True)

    if not duckdb_file_arg:
        return duckdb_dir / "batch.duckdb"

    p = Path(duckdb_file_arg)
    if p.suffix == "":
        p = p.with_suffix(".duckdb")

    if not p.is_absolute():
        p = duckdb_dir / p.name

    return p.resolve()
