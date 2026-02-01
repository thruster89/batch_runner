import pandas as pd
from util.paths import LOG_DIR

SLOW_SQL_STATS = []

def write_slow_sql_top10(batch_date: str):
    if not SLOW_SQL_STATS:
        return

    df = pd.DataFrame(SLOW_SQL_STATS)
    top10 = df.sort_values("elapsed_sec", ascending=False).head(10)

    out = LOG_DIR / f"slow_sql_top10_{batch_date}.csv"
    top10.to_csv(out, index=False, encoding="utf-8-sig")
