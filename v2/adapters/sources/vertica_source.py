# file: v2/adapters/sources/vertica_source.py

import csv
import gzip
from pathlib import Path


def export_sql_to_csv(conn, sql_text, out_file, logger, compression="none", fetch_size=10000):

    cursor = conn.cursor()
    cursor.execute(sql_text)

    if cursor.description is None:
        logger.warning("No result set returned, skipping CSV export")
        cursor.close()
        return 0

    columns = [col[0] for col in cursor.description]

    out_file = Path(out_file)
    tmp_file = out_file.with_suffix(out_file.suffix + ".tmp")
    out_file.parent.mkdir(parents=True, exist_ok=True)

    total_rows = 0

    try:
        if compression == "gzip":
            f = gzip.open(tmp_file, "wt", newline="", encoding="utf-8")
        else:
            f = open(tmp_file, "w", newline="", encoding="utf-8")

        with f:
            writer = csv.writer(f)
            writer.writerow(columns)

            while True:
                rows = cursor.fetchmany(fetch_size)
                if not rows:
                    break

                writer.writerows(rows)
                total_rows += len(rows)

                if total_rows % (fetch_size * 5) == 0:
                    logger.info("CSV progress: %d rows", total_rows)
                    
        tmp_file.replace(out_file)
        logger.debug("File committed: %s", out_file)
        cursor.close()

        logger.info(
            "CSV export completed | rows=%d file=%s",
            total_rows,
            out_file,
        )
    except Exception:
        if tmp_file.exists():
            tmp_file.unlink()
        raise

    return total_rows