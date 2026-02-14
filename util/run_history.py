from pathlib import Path
import csv
from typing import Optional, Set, Tuple

# -------------------------------------------------
# 경로 설정
# -------------------------------------------------
HISTORY_DIR = Path("logs/run_history")
HISTORY_DIR.mkdir(parents=True, exist_ok=True)

# 현재 실행에서 사용할 history 파일
CURRENT_HISTORY_FILE: Optional[Path] = None


# -------------------------------------------------
# 현재 실행용 history 파일 설정
# -------------------------------------------------
def init_run_history(batch_ts: str) -> Path:
    """
    배치 시작 시 호출
    run_history/YYYYMMDD_HHMMSS.csv 생성
    """
    global CURRENT_HISTORY_FILE

    history_file = HISTORY_DIR / f"{batch_ts}.csv"
    CURRENT_HISTORY_FILE = history_file

    return history_file


# -------------------------------------------------
# 실행 이력 기록
# -------------------------------------------------
def append_run_history(row: dict):
    """
    현재 실행 run_history 파일에 기록
    """
    if CURRENT_HISTORY_FILE is None:
        raise RuntimeError("run_history not initialized. Call init_run_history() first.")

    write_header = not CURRENT_HISTORY_FILE.exists()

    with CURRENT_HISTORY_FILE.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "batch_ts",
                "host",
                "sql_file",
                "params",
                "sql_hash",
                "status",
                "rows",
                "elapsed_sec",
                "output_file",
                "error_message",
            ],
        )

        if write_header:
            writer.writeheader()

        writer.writerow(row)


# -------------------------------------------------
# 마지막 실행 파일 찾기
# -------------------------------------------------
def find_latest_history_file() -> Optional[Path]:
    """
    run_history 폴더에서 가장 최근 수정된 파일 반환
    (파일명 정렬이 아니라 실제 수정시간 기준)
    """
    files = list(HISTORY_DIR.glob("*.csv"))

    if not files:
        return None

    files.sort(key=lambda p: p.stat().st_mtime)
    return files[-1]


# -------------------------------------------------
# 마지막 실행 기준 성공 key 로드
# -------------------------------------------------
def load_last_success_keys() -> Set[Tuple[str, str, str, str]]:
    """
    마지막 실행 기준 성공 key 반환
    key = (host, sql_file, params, sql_hash)
    """
    keys: Set[Tuple[str, str, str, str]] = set()

    history_file = find_latest_history_file()
    if history_file is None:
        return keys

    try:
        with history_file.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                if row.get("status") == "OK":
                    keys.add((
                        row.get("host", ""),
                        row.get("sql_file", ""),
                        row.get("params", ""),
                        row.get("sql_hash", ""),
                    ))

    except Exception:
        # history 파일 깨졌거나 읽기 실패 시 안전하게 빈 set 반환
        return set()

    return keys
