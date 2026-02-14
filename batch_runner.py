import warnings
warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable*",
)

import logging
from datetime import datetime
from pathlib import Path

from util.paths import BASE_DIR, FAILED_DIR, resolve_duckdb_file
from util.yaml_loader import load_yaml
from util.logging import setup_logging, cleanup_old_logs
from util.sql_targets import sql_files_to_tables
from util.run_history import init_run_history

from core.args import parse_args, parse_params_override
from core.dryrun import dryrun_check, write_dryrun_report

from oracle.sql_loader import collect_sql_files
from transform.csv_to_excel import csv_to_excel

from duckdb_ops.load_csv import load_csv_to_duckdb
from duckdb_ops.load_parquet import load_parquet_to_duckdb
from duckdb_ops.union_views import create_union_views

from stats.slow_sql import write_slow_sql_top10

from duckdb_ops.run_duckdb_sql_dir import run_duckdb_sql_dir
# =========================================================
# RUN CONTEXT BUILDER
# =========================================================
def build_run_context(args, source_cfg, params):

    # hosts
    if args.hosts:
        run_hosts = [h.strip() for h in args.hosts.split(",")]
        logging.info("HOSTS override: %s", run_hosts)
    else:
        run_hosts = source_cfg["run"]["hosts"]
        logging.info("HOSTS from env.yml: %s", run_hosts)

    # params override
    # if args.params:
        # override = parse_params_override(args.params)
    if args.param:
        override = parse_params_override(args.param)
        params.update(override)
        logging.info("PARAMS override: %s", override)

    logging.info("FINAL PARAMS: %s", params)

    # sql subdirs
    if args.sql_subdirs:
        sql_subdirs = [
            s.strip().replace("\\", "/").strip("/")
            for s in args.sql_subdirs.split(",")
        ]
        logging.info("SQL subdirs filter: %s", sql_subdirs)
    else:
        sql_subdirs = None

    # duckdb file
    duckdb_file = resolve_duckdb_file(args.duckdb_file)
    duckdb_file.parent.mkdir(parents=True, exist_ok=True)
    logging.info("DuckDB file: %s", duckdb_file)

    logging.info("EXPORT FORMAT=%s", args.format)

    return {
        "run_hosts": run_hosts,
        "params": params,
        "sql_subdirs": sql_subdirs,
        "duckdb_file": duckdb_file,
        "format": args.format,
    }


# =========================================================
# MAIN
# =========================================================
def main():
    args = parse_args()
    RUN_MODE = args.mode

    batch_date = datetime.now().strftime("%Y%m%d")
    batch_ts   = datetime.now().strftime("%Y%m%d_%H%M%S")

    setup_logging(batch_date)
    cleanup_old_logs(365)
    init_run_history(batch_ts)

    logging.info("Batch started")
    logging.info("RUN_MODE=%s", RUN_MODE)

    # -----------------------------------------------------
    # LOAD CONFIG
    # -----------------------------------------------------
    env    = load_yaml(BASE_DIR / "config" / "env.yml")
    params = load_yaml(BASE_DIR / "config" / "params.yml")

    source_cfg = env["sources"][args.source]
    hosts_cfg  = source_cfg["hosts"]

    # -----------------------------------------------------
    # EXPORT 함수 선택
    # -----------------------------------------------------
    if args.source == "oracle":
        from oracle.client import init_oracle_client as init_client
        from oracle.export_csv import export_oracle_to_csv as export_csv
        from oracle.export_parquet_stream import export_oracle_to_parquet_stream as export_parquet

    elif args.source == "vertica":
        from vertica.client import init_vertica_client as init_client
        from vertica.export_csv import export_vertica_to_csv as export_csv
        from vertica.export_parquet_stream import export_vertica_to_parquet_stream as export_parquet

    else:
        raise ValueError(f"Unsupported source: {args.source}")

    # init_client(source_cfg)
    if not args.skip_export:
        init_client(source_cfg)
    else:
        logging.info("EXPORT stage skipped (--skip-export)")
    # -----------------------------------------------------
    # CONTEXT
    # -----------------------------------------------------
    ctx = build_run_context(args, source_cfg, params)

    run_hosts     = ctx["run_hosts"]
    params        = ctx["params"]
    sql_subdirs   = ctx["sql_subdirs"]
    DUCKDB_FILE   = ctx["duckdb_file"]
    export_format = ctx["format"]

    FAILED_DIR.mkdir(parents=True, exist_ok=True)

    # =====================================================
    # DRYRUN
    # =====================================================
    if RUN_MODE == "DRYRUN":
        rows = []

        for host in run_hosts:
            sql_files = collect_sql_files(args.source, host, sql_subdirs)

            rows.extend(
                dryrun_check(
                    host,
                    sql_files,
                    params,
                    batch_ts,
                )
            )

        write_dryrun_report(rows, batch_ts)

        if any(r["status"] == "FAIL" for r in rows):
            raise RuntimeError("DRYRUN FAILED")

        return

    # =====================================================
    # ALL / RETRY
    # =====================================================
    tables_by_host = {}

    for host in run_hosts:
        cfg    = hosts_cfg[host]
        schema = cfg.get("duckdb_schema", host)

        sql_files = collect_sql_files(args.source, host, sql_subdirs)
        
        if args.sql_filter:
            patterns = [p.strip().lower() for p in args.sql_filter.split(",")]
            sql_files = [
                f for f in sql_files
                if any(p in f.as_posix().lower() for p in patterns)
            ]
            logging.info("SQL filter applied: %s (%d files)", patterns, len(sql_files))
            
        target_tables = sql_files_to_tables(sql_files)

        tables_by_host[host] = target_tables
        failed_all: list[str] = []

        # -------------------------------------------------
        # EXPORT
        # -------------------------------------------------
        if not args.skip_export and RUN_MODE in ("ALL", "RETRY"):

            retry_mode = (RUN_MODE == "RETRY")

            if export_format == "parquet":
                failed = export_parquet(
                    args.source,
                    host,
                    cfg,
                    sql_files,
                    params,
                    batch_date,
                    retry=retry_mode,
                )
            else:
                failed = export_csv(
                    args.source,
                    host,
                    cfg,
                    sql_files,
                    params,
                    batch_date,
                    retry=retry_mode,
                )

            failed_all.extend(failed)

        # -------------------------------------------------
        # FAILED LIST
        # -------------------------------------------------
        fail_file = FAILED_DIR / f"{args.source}_{host}.lst"

        if failed_all:
            fail_file.write_text("\n".join(sorted(set(failed_all))))
        else:
            if fail_file.exists():
                fail_file.unlink()

        # -------------------------------------------------
        # EXCEL EXPORT
        # -------------------------------------------------
        if not args.no_excel and export_format == "csv":
            csv_to_excel(args.source, host, schema, sql_files)
        else:
            logging.info("Excel export skipped | format=%s", export_format)

        # -------------------------------------------------
        # DUCKDB LOAD
        # -------------------------------------------------
        logging.info("DuckDB LOAD start | schema=%s | tables=%d", schema, len(target_tables))
        if export_format == "parquet":
            load_parquet_to_duckdb(DUCKDB_FILE, schema, target_tables)
        else:
            load_csv_to_duckdb(
                DUCKDB_FILE,
                args.source,
                host,
                schema,
                target_tables,
                params,
            )
        logging.info("DuckDB LOAD finished | schema=%s", schema)            

        # -------------------------------------------------
        # DUCKDB POSTWORK
        # -------------------------------------------------
        if not args.skip_duckdb_sql and args.duckdb_sql_dir:
            run_duckdb_sql_dir(DUCKDB_FILE, Path(args.duckdb_sql_dir), batch_ts, retry=(RUN_MODE == "RETRY"),sql_filter=args.duckdb_sql_filter,)

    # =====================================================
    # UNION / STATS
    # =====================================================
    for host in run_hosts:
        schema = hosts_cfg[host].get("duckdb_schema", host)

        create_union_views(
            DUCKDB_FILE,
            schema,
            tables_by_host[host],
        )

    write_slow_sql_top10(batch_date)

    logging.info("Batch finished")


if __name__ == "__main__":
    main()
