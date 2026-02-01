import time
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from util.paths import CSV_DIR, PARQUET_DIR

STRING_KEYWORDS = ["ID", "CODE", "KEY", "NO", "SEQ"]
NUMERIC_KEYWORDS = ["AMT", "AMOUNT", "RATE", "CNT", "COUNT", "QTY"]


def decide_column_type(col_name: str):
    col = col_name.upper()

    if any(k in col for k in STRING_KEYWORDS):
        return "string"
    if any(k in col for k in NUMERIC_KEYWORDS):
        return "numeric"
    return None


def csv_to_parquet(schema: str):
    """
    CSV 파일명 그대로 Parquet 변환
    (suffix 포함 / 이미 존재하면 SKIP)
    """
    base = CSV_DIR / schema
    out_base = PARQUET_DIR / schema

    for csv in base.rglob("*.csv.gz"):
        rel = csv.relative_to(base)
        out = out_base / rel.with_name(rel.name.replace(".csv.gz", ".parquet"))

        if out.exists():
            logging.info(
                "Parquet exists, skip CSV→Parquet | %s",
                out.as_posix(),
            )
            continue

        out.parent.mkdir(parents=True, exist_ok=True)

        start = time.time()
        try:
            df = pd.read_csv(csv, compression="gzip", low_memory=False)

            fields = []
            arrays = []

            for col in df.columns:
                rule = decide_column_type(col)

                if rule == "string":
                    arr = pa.array(df[col].astype(str))
                    fields.append(pa.field(col, pa.string()))
                elif rule == "numeric":
                    arr = pa.array(pd.to_numeric(df[col], errors="coerce"))
                    fields.append(pa.field(col, pa.float64()))
                else:
                    arr = pa.array(df[col])
                    fields.append(pa.field(col, arr.type))

                arrays.append(arr)

            table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
            pq.write_table(table, out)

            elapsed = round(time.time() - start, 2)
            logging.info(
                "Parquet OK | %s | %.2fs",
                rel.as_posix(),
                elapsed,
            )

        except Exception as e:
            logging.error(
                "Parquet FAIL | %s | %s",
                rel.as_posix(),
                e,
            )
