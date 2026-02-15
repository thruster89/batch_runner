# file: v2/stages/export_stage.py

import time
from pathlib import Path

from v2.adapters.sources.oracle_client import init_oracle_client, get_oracle_conn
from v2.adapters.sources.vertica_client import get_vertica_conn
from v2.engine.path_utils import resolve_path
from v2.engine.sql_utils import sort_sql_files

def sanitize_sql(sql: str) -> str:
    sql = sql.strip()

    while sql.endswith(";") or sql.endswith("/"):
        sql = sql[:-1].rstrip()

    return sql

def _render_sql(sql_text: str, params: dict) -> str:
    for k, v in params.items():
        sql_text = sql_text.replace(f"${{{k}}}", str(v))
        sql_text = sql_text.replace(f":{k}", str(v))
        sql_text = sql_text.replace(f"{{#{k}}}", str(v))
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


# ★ 수정: params 출력용 유틸 추가
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
    out_dir = resolve_path(ctx, export_cfg["out_dir"])

    source_sel = job_cfg.get("source", {})
    source_type = source_sel.get("type", "oracle")
    host_name = source_sel.get("host")

    sql_files = sort_sql_files(sql_dir)

    if not sql_files:
        logger.warning("No SQL files found in %s", sql_dir)
        return

    # --------------------------------------------------
    # ★ PLAN MODE 먼저 처리 (DB 연결 안 함)
    # --------------------------------------------------
    if ctx.mode == "plan":
        plan_file = ctx.work_dir / "logs" / f"plan_{ctx.run_id}.sql"

        with open(plan_file, "w", encoding="utf-8") as pf:
            for idx, sql_file in enumerate(sql_files, 1):
                sql_text = sql_file.read_text(encoding="utf-8")
                
                logger.info("PLAN SQL [%d/%d] : %s", idx, len(sql_files), sql_file.name)
                
                rendered_sql = _render_sql(sql_text, ctx.params)
                if rendered_sql.strip().endswith(";"):
                    logger.warning("SQL ends with semicolon; removing automatically")
                    rendered_sql = sanitize_sql(rendered_sql)

                preview = preview_sql(rendered_sql, ctx.params)

                logger.info("Preview:")
                for line in preview.splitlines():
                    logger.info("  %s", line)
                logger.info("")

                pf.write("-- ======================================\n")
                pf.write(f"-- FILE: {sql_file.name}\n")
                pf.write("-- PARAMS:\n")
                pf.write(format_params(ctx.params))
                pf.write("\n\n")
                pf.write(rendered_sql)
                pf.write("\n\n")

        logger.info("Plan file generated: %s", plan_file)
        return

    # --------------------------------------------------
    # 여기부터 RUN / RETRY
    # --------------------------------------------------
    conn = None

    try:
        # -----------------------------
        # Connect
        # -----------------------------
        if source_type == "oracle":
            from v2.adapters.sources.oracle_source import export_sql_to_csv

            oracle_cfg = env_cfg["sources"]["oracle"]
            fetch_size = oracle_cfg.get("export", {}).get("fetch_size", 10000)

            if not host_name:
                run_hosts = oracle_cfg.get("run", {}).get("hosts", [])
                if not run_hosts:
                    raise RuntimeError("No oracle run hosts configured in env.yml")
                host_name = run_hosts[0]

            host_cfg = oracle_cfg.get("hosts", {}).get(host_name)
            if not host_cfg:
                raise RuntimeError(f"Oracle host not found in env.yml: {host_name}")

            mode = init_oracle_client(oracle_cfg)
            logger.info("Oracle client mode: %s", mode)

            conn = get_oracle_conn(host_cfg)
            logger.info("Oracle connection established")

        elif source_type == "vertica":
            from v2.adapters.sources.vertica_source import export_sql_to_csv

            vertica_cfg = env_cfg["sources"]["vertica"]
            fetch_size = vertica_cfg.get("export", {}).get("fetch_size", 10000)

            if not host_name:
                run_hosts = vertica_cfg.get("run", {}).get("hosts", [])
                if not run_hosts:
                    raise RuntimeError("No vertica run hosts configured in env.yml")
                host_name = run_hosts[0]

            host_cfg = vertica_cfg.get("hosts", {}).get(host_name)
            if not host_cfg:
                raise RuntimeError(f"Vertica host not found in env.yml: {host_name}")

            conn = get_vertica_conn(host_cfg)
            logger.info("Vertica connection established")

        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        # --------------------------------------------------
        # 실행 정보 로그
        # --------------------------------------------------
        fmt = export_cfg.get("format", "csv")
        compression = export_cfg.get("compression", "none")
        overwrite = export_cfg.get("overwrite", False)

        if fmt == "csv" and compression == "gzip":
            ext = "csv.gz"
        elif fmt == "csv":
            ext = "csv"
        elif fmt == "parquet":
            ext = "parquet"
        else:
            raise ValueError("Unsupported format")

        logger.info("-" * 60)
        logger.info("SQL execution order:")
        for f in sql_files:
            logger.info("  %s", f.name)

        logger.info("")
        logger.info("SQL count: %d", len(sql_files))

        logger.info("-" * 60)
        logger.info("Output format: %s", fmt.upper())
        logger.info("Compression: %s", compression.upper())
        logger.info("Overwrite mode: %s", overwrite)
        logger.info("-" * 60)

        # --------------------------------------------------
        # 실행 루프
        # --------------------------------------------------
        for idx, sql_file in enumerate(sql_files, 1):
            logger.info("EXPORT SQL [%d/%d] start: %s", idx, len(sql_files), sql_file.name)

            sql_text = sql_file.read_text(encoding="utf-8")
            rendered_sql = _render_sql(sql_text, ctx.params)
            rendered_sql =  sanitize_sql(rendered_sql)
            
            out_file = out_dir / f"{sql_file.stem}.{ext}"

            if out_file.exists() and not overwrite:
                logger.info("skip (file exists): %s", out_file.name)
                continue

            if out_file.exists() and overwrite:
                logger.info("overwrite: %s", out_file.name)

            start_time = time.time()

            rows = export_sql_to_csv(
                conn=conn,
                sql_text=rendered_sql,
                out_file=out_file,
                logger=logger,
                compression=compression,
                fetch_size=fetch_size,
            )

            elapsed = time.time() - start_time
            size_mb = out_file.stat().st_size / (1024 * 1024) if out_file.exists() else 0

            logger.info(
                "EXPORT SQL [%d/%d] end: %s | rows=%d | size=%.2fMB | elapsed=%.2fs",
                idx,
                len(sql_files),
                sql_file.name,
                rows,
                size_mb,
                elapsed,
            )

    finally:
        if conn:
            try:
                conn.close()
                logger.info("")
                logger.info("DB connection closed")
            except Exception as e:
                logger.warning("DB connection close failed: %s", e)

    logger.debug("EXPORT stage end")
