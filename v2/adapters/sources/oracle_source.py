# file: v2/adapters/sources/oracle_source.py

import csv
import gzip
import time
from pathlib import Path


def export_sql_to_csv(conn, sql_text, out_file, logger, compression="none", fetch_size=10000, stall_seconds=1800):
    """
    fetchmany 기반 고속 CSV export

    stall_seconds:
      - fetch/execute가 예외 없이 멈추는(hang) 케이스 대응용
      - 가능한 경우 Oracle driver의 call_timeout을 설정해서 stall을 예외로 전환
    """

    cursor = conn.cursor()

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
        cursor.close()
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
                # NOTE:
                # - 실제 "멈춤"은 여기 fetchmany가 block 되면서 발생
                # - call_timeout이 동작하면 stall_seconds 경과 시 예외 발생
                rows = cursor.fetchmany(fetch_size)
                if not rows:
                    break

                writer.writerows(rows)
                total_rows += len(rows)

                # 진행 로그는 너무 자주 찍지 않도록(기존 정책 유지)
                if total_rows % (fetch_size * 5) == 0:
                    logger.info("CSV progress: %d rows", total_rows)
                    last_log_ts = time.time()
                else:
                    # 장시간 큰 row 간격에서 로그가 전혀 없으면 운영상 불편하니,
                    # 2분마다 한 번은 보조 로그를 찍어 "살아있음"을 확인
                    now = time.time()
                    if now - last_log_ts >= 120:
                        logger.info("CSV progress: %d rows (heartbeat)", total_rows)
                        last_log_ts = now

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
