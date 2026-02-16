import time
import re
import shutil
from datetime import datetime
from pathlib import Path

from v2.adapters.sources.oracle_client import init_oracle_client, get_oracle_conn
from v2.adapters.sources.vertica_client import get_vertica_conn
from v2.engine.path_utils import resolve_path
from v2.engine.sql_utils import sort_sql_files


def expand_params(params: dict):
    """
    clsYymm=202403,202312
    →
    [{"clsYymm":"202403"}, {"clsYymm":"202312"}]
    """

    multi_keys = []
    values = []

    for k, v in params.items():
        if "," in str(v):
            multi_keys.append(k)
            values.append([x.strip() for x in str(v).split(",")])
        else:
            multi_keys.append(k)
            values.append([v])

    from itertools import product

    expanded = []
    for combo in product(*values):
        expanded.append(dict(zip(multi_keys, combo)))

    return expanded


def sanitize_sql(sql: str) -> str:
    sql = sql.strip()

    while sql.endswith(";") or sql.endswith("/"):
        sql = sql[:-1].rstrip()

    return sql


def build_csv_name(sqlname: str, host: str, params: dict, ext: str) -> str:
    parts = [sqlname]

    if host:
        parts.append(host)

    for k in sorted(params.keys()):
        v = str(params[k]).replace(" ", "_")
        parts.append(f"{k}_{v}")

    return "__".join(parts) + f".{ext}"


def backup_existing_file(file_path: Path, backup_dir: Path, keep: int = 10):
    if not file_path.exists():
        return

    backup_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = file_path.stem + f"__{ts}" + file_path.suffix
    target = backup_dir / backup_name

    shutil.move(str(file_path), str(target))

    prefix = file_path.stem + "__"
    backups = sorted(
        backup_dir.glob(prefix + "*.csv"),
        key=lambda p: p.stat().st_mtime
    )

    while len(backups) > keep:
        backups[0].unlink()
        backups.pop(0)


def _render_sql(sql_text: str, params: dict) -> str:
    if not params:
        return sql_text

    for k in sorted(params.keys(), key=len, reverse=True):
        v = str(params[k])

        sql_text = sql_text.replace(f"${{{k}}}", v)
        sql_text = sql_text.replace(f"{{#{k}}}", v)

        pattern = re.compile(rf'(?<!:):{re.escape(k)}\b')
        sql_text = pattern.sub(v, sql_text)

    return sql_text


def preview_sql(sql_text, params, context=5):
    lines = sql_text.splitlines()
    hit_lines = []

    for i, line in enumerate(lines):
        for v in params.values():
            if str(v) in line:
                hit_lines.append(i)

    if not hit_lines:
        return "\n".join(lines[:10])

    start = max(0, min(hit_lines) - context)
    end = min(len(lines), max(hit_lines) + context + 1)

    return "\n".join(lines[start:end])


def format_params(params: dict) -> str:
    lines = []
    for k, v in params.items():
        lines.append(f"--   {k} = {v}")
    return "\n".join(lines)


def run(ctx):
    logger = ctx.logger
    job_cfg = ctx.job_config
    env_cfg = ctx.env_config

    export_cfg = job_cfg.get("export")
    if not export_cfg:
        logger.info("EXPORT stage skipped (no config)")
        return

    sql_dir = resolve_path(ctx, export_cfg["sql_dir"])
    out_dir = resolve_path(ctx, export_cfg["out_dir"]) / ctx.job_name
    out_dir.mkdir(parents=True, exist_ok=True)

    source_sel = job_cfg.get("source", {})
    source_type = source_sel.get("type", "oracle")
    host_name = source_sel.get("host")

    sql_files = sort_sql_files(sql_dir)
    if not sql_files:
        logger.warning("No SQL files found in %s", sql_dir)
        return

    param_sets = expand_params(ctx.params)

    # PLAN MODE
    if ctx.mode == "plan":
        log_dir = ctx.work_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        plan_file = log_dir / f"plan_{ctx.run_id}.sql"

        with open(plan_file, "w", encoding="utf-8") as pf:
            for idx, sql_file in enumerate(sql_files, 1):
                for param_idx, param_set in enumerate(param_sets, 1):

                    sql_text = sql_file.read_text(encoding="utf-8")

                    logger.info(
                        "PLAN SQL [%d/%d] param[%d/%d] : %s | %s",
                        idx, len(sql_files),
                        param_idx, len(param_sets),
                        sql_file.name,
                        param_set
                    )

                    rendered_sql = sanitize_sql(_render_sql(sql_text, param_set))
                    preview = preview_sql(rendered_sql, param_set)

                    logger.info("Preview:")
                    for line in preview.splitlines():
                        logger.info("  %s", line)
                    logger.info("")

                    pf.write("-- ======================================\n")
                    pf.write(f"-- FILE: {sql_file.name}\n")
                    pf.write("-- PARAMS:\n")
                    pf.write(format_params(param_set))
                    pf.write("\n\n")
                    pf.write(rendered_sql)
                    pf.write("\n\n")

        logger.info("Plan file generated: %s", plan_file)
        return

    fmt = export_cfg.get("format", "csv")
    compression = export_cfg.get("compression", "none")
    overwrite = export_cfg.get("overwrite", False)
    backup_keep = export_cfg.get("backup_keep", 10)

    if fmt == "csv" and compression == "gzip":
        ext = "csv.gz"
    elif fmt == "csv":
        ext = "csv"
    elif fmt == "parquet":
        ext = "parquet"
    else:
        raise ValueError("Unsupported format")

    stall_seconds = 30 * 60
    max_attempts = 3

    conn = None
    export_sql_to_csv = None
    fetch_size = 10000
    oracle_client_initialized = False

    def _connect():
        nonlocal conn, export_sql_to_csv, fetch_size, host_name, oracle_client_initialized

        reconnecting = conn is not None

        if conn:
            try:
                conn.close()
            except Exception:
                pass
            conn = None

        if source_type == "oracle":
            from v2.adapters.sources.oracle_source import export_sql_to_csv as _export

            oracle_cfg = env_cfg["sources"]["oracle"]
            fetch_size = oracle_cfg.get("export", {}).get("fetch_size", 10000)

            if not host_name:
                run_hosts = oracle_cfg.get("run", {}).get("hosts", [])
                if not run_hosts:
                    raise RuntimeError("No oracle run hosts configured")
                host_name = run_hosts[0]

            host_cfg = oracle_cfg.get("hosts", {}).get(host_name)
            if not host_cfg:
                raise RuntimeError(f"Oracle host not found: {host_name}")

            if not oracle_client_initialized:
                init_oracle_client(oracle_cfg)
                oracle_client_initialized = True

            conn = get_oracle_conn(host_cfg)
            export_sql_to_csv = _export

            logger.info("Oracle connection established" if not reconnecting else "Oracle reconnected")

        elif source_type == "vertica":
            from v2.adapters.sources.vertica_source import export_sql_to_csv as _export

            vertica_cfg = env_cfg["sources"]["vertica"]
            fetch_size = vertica_cfg.get("export", {}).get("fetch_size", 10000)

            if not host_name:
                run_hosts = vertica_cfg.get("run", {}).get("hosts", [])
                if not run_hosts:
                    raise RuntimeError("No vertica run hosts configured")
                host_name = run_hosts[0]

            host_cfg = vertica_cfg.get("hosts", {}).get(host_name)
            if not host_cfg:
                raise RuntimeError(f"Vertica host not found: {host_name}")

            conn = get_vertica_conn(host_cfg)
            export_sql_to_csv = _export

            logger.info("Vertica connection established" if not reconnecting else "Vertica reconnected")

        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    try:
        _connect()

        logger.info("-" * 60)
        logger.info("SQL execution order:")
        for f in sql_files:
            logger.info("  %s", f.name)

        logger.info("SQL count: %d", len(sql_files))
        logger.info("Output format: %s", fmt.upper())
        logger.info("Compression: %s", compression.upper())
        logger.info("Overwrite mode: %s", overwrite)
        logger.info("Run mode: %s", ctx.mode)
        logger.info("-" * 60)

        for idx, sql_file in enumerate(sql_files, 1):
            for param_idx, param_set in enumerate(param_sets, 1):

                csv_name = build_csv_name(
                    sqlname=sql_file.stem,
                    host=host_name,
                    params=param_set,
                    ext=ext,
                )

                out_file = out_dir / csv_name
                tmp_file = out_file.with_suffix(out_file.suffix + ".tmp")

                if out_file.exists() and not tmp_file.exists():
                    if ctx.mode == "run" and overwrite:
                        logger.info("overwrite enabled: %s", out_file.name)
                        backup_dir = out_dir / "_backup"
                        backup_existing_file(out_file, backup_dir, keep=backup_keep)
                    else:
                        logger.info("skip (already completed): %s", out_file.name)
                        continue

                attempts = 0

                while attempts < max_attempts:
                    attempts += 1

                    logger.info(
                        "EXPORT SQL [%d/%d] param[%d/%d] attempt %d/%d start: %s | %s",
                        idx, len(sql_files),
                        param_idx, len(param_sets),
                        attempts, max_attempts,
                        sql_file.name,
                        param_set
                    )

                    try:
                        sql_text = sql_file.read_text(encoding="utf-8")
                        rendered_sql = sanitize_sql(_render_sql(sql_text, param_set))

                        start_time = time.time()

                        rows = export_sql_to_csv(
                            conn=conn,
                            sql_text=rendered_sql,
                            out_file=out_file,
                            logger=logger,
                            compression=compression,
                            fetch_size=fetch_size,
                            stall_seconds=stall_seconds,
                        )

                        rows = rows or 0
                        elapsed = time.time() - start_time
                        size_mb = out_file.stat().st_size / (1024 * 1024) if out_file.exists() else 0

                        logger.info(
                            "EXPORT SQL [%d/%d] done: rows=%d size=%.2fMB elapsed=%.2fs",
                            idx, len(sql_files), rows, size_mb, elapsed
                        )

                        break

                    except Exception as e:
                        logger.error(
                            "EXPORT SQL [%d/%d] attempt %d/%d FAILED: %s",
                            idx, len(sql_files), attempts, max_attempts, e
                        )

                        if attempts < max_attempts:
                            logger.info("Reconnecting and retrying...")
                            _connect()
                        else:
                            raise

    finally:
        if conn:
            try:
                conn.close()
                logger.info("DB connection closed")
            except Exception as e:
                logger.warning("DB connection close failed: %s", e)

    logger.debug("EXPORT stage end")
