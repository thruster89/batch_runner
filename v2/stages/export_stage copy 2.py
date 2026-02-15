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
    """
    안전한 치환:
    - 키 길이 긴 것부터 치환 (id vs id2 등 prefix 충돌 방지)
    """
    if not params:
        return sql_text

    for k in sorted(params.keys(), key=len, reverse=True):
        v = params[k]
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

    # 출력 디렉토리 보장
    out_dir.mkdir(parents=True, exist_ok=True)

    source_sel = job_cfg.get("source", {})
    source_type = source_sel.get("type", "oracle")
    host_name = source_sel.get("host")

    sql_files = sort_sql_files(sql_dir)
    if not sql_files:
        logger.warning("No SQL files found in %s", sql_dir)
        return

    # --------------------------------------------------
    # PLAN MODE (DB 연결 안 함)
    # --------------------------------------------------
    if ctx.mode == "plan":
        log_dir = ctx.work_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        plan_file = log_dir / f"plan_{ctx.run_id}.sql"

        with open(plan_file, "w", encoding="utf-8") as pf:
            for idx, sql_file in enumerate(sql_files, 1):
                sql_text = sql_file.read_text(encoding="utf-8")

                logger.info("PLAN SQL [%d/%d] : %s", idx, len(sql_files), sql_file.name)

                rendered_sql = _render_sql(sql_text, ctx.params)
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
    # RUN / RETRY
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

    # 30분 stall 정책 + 최대 3회 재시도 정책
    stall_seconds = 30 * 60
    max_attempts = 3

    conn = None
    export_sql_to_csv = None
    fetch_size = 10000

    # reconnect에 필요한 설정 보관
    _host_cfg = None
    _oracle_cfg = None
    _vertica_cfg = None

    def _connect():
        nonlocal conn, export_sql_to_csv, fetch_size, host_name, _host_cfg, _oracle_cfg, _vertica_cfg

        # 기존 연결 닫기
        if conn:
            try:
                conn.close()
            except Exception:
                pass
            conn = None

        if source_type == "oracle":
            from v2.adapters.sources.oracle_source import export_sql_to_csv as _export

            _oracle_cfg = env_cfg["sources"]["oracle"]
            fetch_size = _oracle_cfg.get("export", {}).get("fetch_size", 10000)

            if not host_name:
                run_hosts = _oracle_cfg.get("run", {}).get("hosts", [])
                if not run_hosts:
                    raise RuntimeError("No oracle run hosts configured in env.yml")
                host_name = run_hosts[0]

            _host_cfg = _oracle_cfg.get("hosts", {}).get(host_name)
            if not _host_cfg:
                raise RuntimeError(f"Oracle host not found in env.yml: {host_name}")

            init_oracle_client(_oracle_cfg)
            conn = get_oracle_conn(_host_cfg)

            # connection 레벨 call_timeout도 함께 세팅 (가능한 경우)
            if hasattr(conn, "call_timeout"):
                try:
                    conn.call_timeout = stall_seconds * 1000
                except Exception:
                    pass

            export_sql_to_csv = _export
            logger.info("Oracle connection established")

        elif source_type == "vertica":
            from v2.adapters.sources.vertica_source import export_sql_to_csv as _export

            _vertica_cfg = env_cfg["sources"]["vertica"]
            fetch_size = _vertica_cfg.get("export", {}).get("fetch_size", 10000)

            if not host_name:
                run_hosts = _vertica_cfg.get("run", {}).get("hosts", [])
                if not run_hosts:
                    raise RuntimeError("No vertica run hosts configured in env.yml")
                host_name = run_hosts[0]

            _host_cfg = _vertica_cfg.get("hosts", {}).get(host_name)
            if not _host_cfg:
                raise RuntimeError(f"Vertica host not found in env.yml: {host_name}")

            conn = get_vertica_conn(_host_cfg)
            export_sql_to_csv = _export
            logger.info("Vertica connection established")

        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    try:
        # 최초 연결
        _connect()

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
        logger.info("Run mode: %s", ctx.mode)
        logger.info("Stall watchdog: %ds", stall_seconds)
        logger.info("Max attempts: %d", max_attempts)
        logger.info("-" * 60)

        # --------------------------------------------------
        # 실행 루프
        # --------------------------------------------------
        for idx, sql_file in enumerate(sql_files, 1):
            out_file = out_dir / f"{sql_file.stem}.{ext}"
            tmp_file = out_file.with_suffix(out_file.suffix + ".tmp")

            # 공통 시작 조건: "성공 파일 skip", "tmp 존재 또는 out_file 없음부터 실행"
            # - out_file 있고 tmp_file 없으면 "성공"으로 간주하고 skip
            if out_file.exists() and not tmp_file.exists():
                # 단, RUN 모드에서 overwrite=True이면 강제 재생성 허용
                if ctx.mode == "run" and overwrite:
                    logger.info("overwrite enabled (will re-export): %s", out_file.name)
                else:
                    logger.info("skip (already completed): %s", out_file.name)
                    continue

            # out_file 없거나 tmp_file 존재면 실행 대상
            attempts = 0
            last_err = None

            while attempts < max_attempts:
                attempts += 1

                logger.info(
                    "EXPORT SQL [%d/%d] attempt %d/%d start: %s",
                    idx, len(sql_files), attempts, max_attempts, sql_file.name
                )

                try:
                    sql_text = sql_file.read_text(encoding="utf-8")
                    rendered_sql = _render_sql(sql_text, ctx.params)
                    rendered_sql = sanitize_sql(rendered_sql)

                    start_time = time.time()

                    # Oracle stall watchdog는 oracle_source 내부에서 강제 예외 발생시키도록 구현됨
                    rows = export_sql_to_csv(
                        conn=conn,
                        sql_text=rendered_sql,
                        out_file=out_file,
                        logger=logger,
                        compression=compression,
                        fetch_size=fetch_size,
                        stall_seconds=stall_seconds,  # oracle_source에서 받도록 패치(아래 파일 참고)
                    )

                    # rows None 안전 처리
                    if rows is None:
                        rows = 0

                    elapsed = time.time() - start_time
                    size_mb = out_file.stat().st_size / (1024 * 1024) if out_file.exists() else 0

                    logger.info(
                        "EXPORT SQL [%d/%d] attempt %d/%d end: %s | rows=%d | size=%.2fMB | elapsed=%.2fs",
                        idx, len(sql_files), attempts, max_attempts, sql_file.name, rows, size_mb, elapsed
                    )

                    # 성공 시 다음 SQL로
                    last_err = None
                    break

                except Exception as e:
                    last_err = e
                    logger.error(
                        "EXPORT SQL [%d/%d] attempt %d/%d FAILED: %s | err=%s",
                        idx, len(sql_files), attempts, max_attempts, sql_file.name, e
                    )

                    # 마지막 시도가 아니면 reconnect 후 같은 SQL 재실행
                    if attempts < max_attempts:
                        logger.info("Reconnect and retry same SQL: %s", sql_file.name)
                        try:
                            _connect()
                        except Exception as e2:
                            logger.error("Reconnect FAILED: %s", e2)
                            raise
                    else:
                        # 3회 실패면 즉시 종료(다음 SQL 진행 X)
                        logger.error("Max attempts reached. Abort pipeline at: %s", sql_file.name)
                        raise

    finally:
        if conn:
            try:
                conn.close()
                logger.info("")
                logger.info("DB connection closed")
            except Exception as e:
                logger.warning("DB connection close failed: %s", e)

    logger.debug("EXPORT stage end")
