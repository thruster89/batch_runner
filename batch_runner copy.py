# batch_runner.py (관련 부분만이 아니라 "덮어쓰기용"으로 전체 주길 원하면 말해줘.
# 지금은 duckdb 정리 파트만 정확히 박아준다.)

import warnings
warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable*",
)

import logging
from datetime import datetime

from util.paths import BASE_DIR, FAILED_DIR, resolve_duckdb_file
from util.yaml_loader import load_yaml
from util.logging import setup_logging, cleanup_old_logs
from util.sql_targets import sql_files_to_tables

from core.args import parse_args, parse_params_override
from core.dryrun import dryrun_check, write_dryrun_report

from oracle.client import init_oracle_thick
from oracle.sql_loader import collect_sql_files
from oracle.export_csv import export_oracle_to_csv
from oracle.export_parquet_stream import export_oracle_to_parquet_stream

from transform.csv_to_parquet import csv_to_parquet
from transform.csv_to_excel import csv_to_excel

from duckdb_ops.load_csv import load_csv_to_duckdb
from duckdb_ops.load_parquet import load_parquet_to_duckdb
from duckdb_ops.union_views import create_union_views

from stats.slow_sql import write_slow_sql_top10


def main():
    args = parse_args()
    RUN_MODE = args.mode.upper()

    batch_date = datetime.now().strftime("%Y%m%d")
    batch_ts   = datetime.now().strftime("%Y%m%d_%H%M%S")

    setup_logging(batch_date)
    cleanup_old_logs(365)

    logging.info("Batch started")
    logging.info("RUN_MODE=%s", RUN_MODE)

    env    = load_yaml(BASE_DIR / "config" / "env.yml")
    params = load_yaml(BASE_DIR / "config" / "params.yml")

    # -------------------------
    # HOSTS
    # -------------------------
    if args.hosts:
        run_hosts = [h.strip() for h in args.hosts.split(",")]
        logging.info("HOSTS override: %s", run_hosts)
    else:
        run_hosts = env["oracle"]["run"]["hosts"]
        logging.info("HOSTS from env.yml: %s", run_hosts)

    # -------------------------
    # PARAMS
    # -------------------------
    if args.params:
        override = parse_params_override(args.params)
        params.update(override)
        logging.info("PARAMS override: %s", override)

    logging.info("FINAL PARAMS: %s", params)

    # -------------------------
    # DUCKDB FILE RESOLVE (완전 정리 핵심)
    # -------------------------
    duckdb_file = resolve_duckdb_file(args.duckdb_file)
    logging.info("DuckDB file: %s", duckdb_file.as_posix())

    # -------------------------
    # SQL SUBDIR FILTER
    # -------------------------
    if args.sql_subdirs:
        sql_subdirs = [
            s.strip().replace("\\", "/").strip("/")
            for s in args.sql_subdirs.split(",")
        ]
        logging.info("SQL subdirs filter: %s", sql_subdirs)
    else:
        sql_subdirs = None

    hosts_cfg = env["oracle"]["hosts"]
    FAILED_DIR.mkdir(parents=True, exist_ok=True)

    # =====================================================
    # DRYRUN
    # =====================================================
    if RUN_MODE == "DRYRUN":
        rows = []
        for host in run_hosts:
            cfg    = hosts_cfg[host]
            schema = cfg["duckdb_schema"]

            sql_files = collect_sql_files(schema, sql_subdirs)

            # dryrun은 “실제 실행”이 아니라서 duckdb_file과 무관
            rows.extend(dryrun_check(host, sql_files, params, batch_ts))

        write_dryrun_report(rows, batch_ts)
        if any(r["status"] == "FAIL" for r in rows):
            raise RuntimeError("DRYRUN FAILED")
        return

    # =====================================================
    # ALL / RETRY
    # =====================================================
    init_oracle_thick(env)

    for host in run_hosts:
        cfg    = hosts_cfg[host]
        schema = cfg["duckdb_schema"]

        sql_files = collect_sql_files(schema, sql_subdirs)
        target_tables = sql_files_to_tables(sql_files, schema)

        failed_all = []

        # -------------------------
        # ORACLE → CSV / PARQUET
        # -------------------------
        if RUN_MODE == "ALL":
            if args.export_parquet_direct:
                failed = export_oracle_to_parquet_stream(
                    host, cfg, sql_files, params, batch_date
                )
            else:
                failed = export_oracle_to_csv(
                    host, cfg, sql_files, params, batch_date
                )
            failed_all.extend(failed)

        # -------------------------
        # FAILED LIST
        # -------------------------
        fail_file = FAILED_DIR / f"{host}.lst"
        if failed_all:
            fail_file.write_text("\n".join(sorted(set(failed_all))))
        else:
            if fail_file.exists():
                fail_file.unlink()

        # -------------------------
        # CSV → PARQUET
        # -------------------------
        if args.export_parquet and not args.export_parquet_direct:
            csv_to_parquet(schema)

        # -------------------------
        # DUCKDB LOAD (duckdb_file 전달)
        # -------------------------
        if args.duckdb_source == "parquet":
            load_parquet_to_duckdb(schema, target_tables, duckdb_file)
        else:
            load_csv_to_duckdb(schema, target_tables, duckdb_file)

        # -------------------------
        # CSV → EXCEL
        # -------------------------
        csv_to_excel(schema, sql_files)

    # =====================================================
    # UNION / STATS
    # =====================================================
    # create_union_views도 duckdb_file을 사용한다면 인자 추가하는 게 맞다.
    # (union_views.py가 duckdb.connect(DUCKDB_FILE) 같은 구조면 동일하게 바꿔야 함)
    create_union_views(
        [hosts_cfg[h]["duckdb_schema"] for h in run_hosts],
        duckdb_file,   # <= union_views.py도 이 인자 받게 수정 필요
    )

    write_slow_sql_top10(batch_date)

    logging.info("Batch finished")


if __name__ == "__main__":
    main()
