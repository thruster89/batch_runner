import csv
import gzip
import time
from pathlib import Path


def export_sql_to_csv(
    conn,
    sql_text,
    out_file,
    logger,
    compression="none",
    fetch_size=10000,
    stall_seconds=1800,
):
    """
    fetchmany 기반 고속 CSV export

    stall_seconds:
      - fetch/execute가 예외 없이 멈추는(hang) 케이스 대응용
      - 가능한 경우 Oracle driver의 call_timeout을 설정해서 stall을 예외로 전환
    """

    cursor = conn.cursor()

    try:
        # fetch 성능
        cursor.arraysize = fetch_size

        # stall 대응: call_timeout (가능한 경우만)
        # - python-oracledb에서 ms 단위
        call_timeout_ms = int(stall_seconds * 1000)

        # connection 레벨
        if hasattr(conn, "call_timeout"):
            try:
                conn.call_timeout = call_timeout_ms
            except Exception:
                pass

        # cursor 레벨(지원되는 경우)
        if hasattr(cursor, "call_timeout"):
            try:
                cursor.call_timeout = call_timeout_ms
            except Exception:
                pass

        cursor.execute(sql_text)

        if cursor.description is None:
            logger.warning("No result set returned, skipping CSV export")
            return 0

        columns = [col[0] for col in cursor.description]

        out_file = Path(out_file)
        tmp_file = out_file.with_suffix(out_file.suffix + ".tmp")
        out_file.parent.mkdir(parents=True, exist_ok=True)

        total_rows = 0
        last_log_ts = time.time()

        try:
            if compression == "gzip":
                f = gzip.open(tmp_file, "wt", newline="", encoding="utf-8")
            else:
                f = open(tmp_file, "w", newline="", encoding="utf-8")

            with f:
                writer = csv.writer(f)
                writer.writerow(columns)

                while True:
                    # fetchmany block 구간
                    rows = cursor.fetchmany(fetch_size)
                    if not rows:
                        break

                    writer.writerows(rows)
                    total_rows += len(rows)

                    # 진행 로그
                    if total_rows % (fetch_size * 5) == 0:
                        logger.info("CSV progress: %d rows", total_rows)
                        last_log_ts = time.time()
                    else:
                        # heartbeat 로그 (2분 간격)
                        now = time.time()
                        if now - last_log_ts >= 120:
                            logger.info("CSV progress: %d rows (heartbeat)", total_rows)
                            last_log_ts = now

            tmp_file.replace(out_file)
            logger.debug("File committed: %s", out_file)

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

    finally:
        # cursor 누수 방지 (핵심 보강 포인트)
        try:
            cursor.close()
        except Exception:
            pass
