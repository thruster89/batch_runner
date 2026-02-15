# file: v2/stages/export_stage.py
import time
from pathlib import Path

from v2.adapters.sources.oracle_client import init_oracle_client, get_oracle_conn
from v2.adapters.sources.vertica_client import get_vertica_conn
from v2.engine.path_utils import resolve_path
from v2.engine.sql_utils import sort_sql_files


def _render_sql(sql_text: str, params: dict) -> str:
    for k, v in params.items():
        sql_text = sql_text.replace(f"${{{k}}}", str(v))
        sql_text = sql_text.replace(f":{k}", str(v))
        sql_text = sql_text.replace(f"{{#{k}}}", str(v))
    return sql_text

def preview_sql(sql_text, params, context=5):
    lines = sql_text.splitlines()

    hit_idx = None
    for i, line in enumerate(lines):
        for v in params.values():
            if str(v) in line:
                hit_idx = i
                break
        if hit_idx is not None:
            break

    if hit_idx is None:
        return "\n".join(lines[:10])

    start = max(0, hit_idx - context)
    end = min(len(lines), hit_idx + context + 1)

    return "\n".join(lines[start:end])



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

    # ctx.logger.info("WORK DIR = %s", ctx.work_dir.resolve())
    # logger.info("SQL DIR = %s (exists=%s)", sql_dir, sql_dir.exists())
    # logger.info("OUT DIR = %s", out_dir)

    # job.yml에서 source/host 선택 (없으면 env run.hosts[0])
    source_sel = job_cfg.get("source", {})
    source_type = source_sel.get("type", "oracle")
    host_name = source_sel.get("host")

    conn = None

    try:
        # -----------------------------
        # 1) Connect once (source별)
        # -----------------------------
        if source_type == "oracle":
            from v2.adapters.sources.oracle_source import export_sql_to_csv

            oracle_cfg = env_cfg["sources"]["oracle"]
            fetch_size = (oracle_cfg.get("export", {}).get("fetch_size", 10000))
                
            if not host_name:
                run_hosts = oracle_cfg.get("run", {}).get("hosts", [])
                if not run_hosts:
                    raise RuntimeError("No oracle run hosts configured in env.yml")
                host_name = run_hosts[0]

            host_cfg = oracle_cfg.get("hosts", {}).get(host_name)
            if not host_cfg:
                raise RuntimeError(f"Oracle host not found in env.yml: {host_name}")

            logger.debug("Oracle host selected: %s", host_name)

            mode = init_oracle_client(oracle_cfg)
            logger.debug("Oracle mode detected: %s", mode)

            conn = get_oracle_conn(host_cfg)
            logger.info("Oracle connection established")

        elif source_type == "vertica":
            from v2.adapters.sources.vertica_source import export_sql_to_csv

            vertica_cfg = env_cfg["sources"]["vertica"]
            fetch_size = (vertica_cfg.get("export", {}).get("fetch_size", 10000))
            if not host_name:
                run_hosts = vertica_cfg.get("run", {}).get("hosts", [])
                if not run_hosts:
                    raise RuntimeError("No vertica run hosts configured in env.yml")
                host_name = run_hosts[0]

            host_cfg = vertica_cfg.get("hosts", {}).get(host_name)
            if not host_cfg:
                raise RuntimeError(f"Vertica host not found in env.yml: {host_name}")

            logger.info("Vertica host selected: %s", host_name)

            conn = get_vertica_conn(host_cfg)
            logger.info("Vertica connection established")

        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        # -----------------------------
        # 2) SQL loop
        # -----------------------------
        sql_files = sort_sql_files(sql_dir)
        
        if ctx.mode == "plan":
            plan_file = ctx.work_dir / "logs" / f"plan_{ctx.run_id}.sql"

            with open(plan_file, "w", encoding="utf-8") as pf:
                for idx, sql_file in enumerate(sql_files, 1):
                    sql_text = sql_file.read_text(encoding="utf-8")
                    rendered_sql = _render_sql(sql_text, ctx.params)

                    preview = preview_sql(rendered_sql, ctx.params)

                    logger.info("PLAN SQL [%d/%d] : %s", idx, len(sql_files), sql_file.name)
                    logger.info("Preview:\n%s", preview)

                    pf.write("-- ======================================\n")
                    pf.write(f"-- FILE: {sql_file.name}\n")
                    # pf.write(f"-- OUTPUT: {out_file}\n")
                    pf.write("-- PARAMS:\n")
                    # pf.write(format_params(ctx.params))
                    pf.write("\n\n")
                    pf.write(rendered_sql)
                    pf.write("\n\n")

            logger.info("Plan file generated: %s", plan_file)
            return
        
        logger.info("")
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
        if not sql_files:
            logger.warning("No SQL files found in %s", sql_dir)
            return
            
        logger.info("")
        logger.info("SQL count: %d", len(sql_files))

        logger.info("-" * 60)
        logger.info("Output format: %s", fmt.upper())
        logger.info("Compression: %s", compression.upper())
        logger.info("Overwrite mode: %s", overwrite)        
        logger.info("-" * 60)
        
        v_preview_n = 10
        
        for idx, sql_file in enumerate(sql_files, 1):
            logger.info("EXPORT SQL [%d/%d] start: %s", idx, len(sql_files), sql_file.name)

            sql_text = sql_file.read_text(encoding="utf-8")
            rendered_sql = _render_sql(sql_text, ctx.params)

            out_file = out_dir / f"{sql_file.stem}.{ext}"

            if out_file.exists() and not overwrite:
                logger.info(
                    "EXPORT SQL [%d/%d] skip (file exists): %s",
                    idx,
                    len(sql_files),
                    out_file.name,
                )
                continue

            if out_file.exists() and overwrite:
                logger.info(
                    "EXPORT SQL [%d/%d] overwrite: %s",
                    idx,
                    len(sql_files),
                    out_file.name,
                )
                
            # logger.info(ctx.mode)
            if ctx.mode == "plan":
                logger.info("------------------------------------------------------------")
                logger.info("DRYRUN SQL [%d/%d] : %s", idx, len(sql_files), sql_file.name)
                logger.info("OUTPUT : %s", out_file)
                logger.info("SQL PREVIEW (rendered, first %d lines):", v_preview_n)
                # logger.info("\n%s", _preview_lines(rendered_sql, v_preview_n))
                continue

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

            if elapsed < 1:
                elapsed_str = f"{elapsed:.2f}s"
            else:
                elapsed_str = f"{elapsed:.1f}s"

            size_mb = 0.0
            if out_file.exists():
                size_mb = out_file.stat().st_size / (1024 * 1024)

            logger.info(
                "EXPORT SQL [%d/%d] end: %s | rows=%d | size=%.2fMB | elapsed=%s",
                idx,
                len(sql_files),
                sql_file.name,
                rows,
                size_mb,
                elapsed_str,
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
