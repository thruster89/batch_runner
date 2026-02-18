# file: v2/stages/load_stage.py

import hashlib
import time
from datetime import datetime
from pathlib import Path

from v2.engine.path_utils import resolve_path
from v2.engine.sql_utils import sort_sql_files, resolve_table_name, extract_sqlname_from_csv


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def run(ctx):
    logger = ctx.logger
    job_cfg = ctx.job_config

    logger.info("LOAD stage start")

    # plan 모드: 실제 적재 없이 스킵
    if ctx.mode == "plan":
        logger.info("LOAD stage skipped (plan mode)")
        logger.info("LOAD stage end")
        return

    export_cfg = job_cfg.get("export", {})
    if not export_cfg:
        logger.info("LOAD stage skipped (no export config)")
        logger.info("LOAD stage end")
        return

    target_cfg = job_cfg.get("target", {})
    if not target_cfg:
        logger.info("LOAD stage skipped (no target config)")
        logger.info("LOAD stage end")
        return

    # export 결과 폴더: out_dir/job_name 우선, 없으면 out_dir 그대로 fallback
    export_base = resolve_path(ctx, export_cfg.get("out_dir", "data/export"))
    export_dir = export_base / ctx.job_name
    if not export_dir.exists():
        export_dir = export_base

    # _backup 폴더 제외하고 csv / csv.gz 파일 수집
    csv_files = sorted([
        p for p in export_dir.iterdir()
        if p.is_file() and p.name.endswith((".csv", ".csv.gz"))
    ])
    if not csv_files:
        logger.warning("No CSV/CSV.GZ files found in %s", export_dir)
        logger.info("LOAD stage end")
        return

    # SQL -> table_name 매핑 준비
    sql_dir = resolve_path(ctx, export_cfg.get("sql_dir", "sql/export"))
    sql_files = sort_sql_files(sql_dir)
    sql_map = {p.stem: p for p in sql_files}  # stem 기준

    tgt_type = (target_cfg.get("type") or "").strip().lower()

    logger.info("LOAD target type=%s | csv_count=%d", tgt_type, len(csv_files))

    # ----------------------------------------
    # Adapter 선택 및 연결
    # ----------------------------------------
    if tgt_type == "duckdb":
        from v2.adapters.targets.duckdb_target import connect, load_csv, _ensure_history

        db_path = resolve_path(ctx, target_cfg.get("db_path", "data/local/result.duckdb"))
        db_path.parent.mkdir(parents=True, exist_ok=True)

        con = connect(db_path)
        _ensure_history(con)

        try:
            _run_load_loop(ctx, logger, csv_files, sql_map, tgt_type,
                           load_fn=lambda table, csv_path, file_hash:
                               load_csv(con, ctx.job_name, table, csv_path, file_hash, ctx.mode))
        finally:
            con.close()

    elif tgt_type == "sqlite3":
        from v2.adapters.targets.sqlite_target import connect, load_csv, _ensure_history

        db_path = resolve_path(ctx, target_cfg.get("db_path", "data/local/result.sqlite"))
        db_path.parent.mkdir(parents=True, exist_ok=True)

        con = connect(db_path)
        _ensure_history(con)

        try:
            _run_load_loop(ctx, logger, csv_files, sql_map, tgt_type,
                           load_fn=lambda table, csv_path, file_hash:
                               load_csv(con, ctx.job_name, table, csv_path, file_hash, ctx.mode))
        finally:
            con.close()

    elif tgt_type == "oracle":
        from v2.adapters.targets.oracle_target import connect, load_csv

        conn = connect(ctx.env_config)

        try:
            _run_load_loop(ctx, logger, csv_files, sql_map, tgt_type,
                           load_fn=lambda table, csv_path, file_hash:
                               load_csv(conn, ctx.job_name, table, csv_path, file_hash, ctx.mode))
        finally:
            conn.close()

    else:
        raise ValueError(f"Unsupported target type: {tgt_type}")

    logger.info("LOAD stage end")


def _run_load_loop(ctx, logger, csv_files, sql_map, tgt_type, load_fn):
    """
    공통 CSV 순회 + 적재 루프.
    load_fn(table_name, csv_path, file_hash) -> int (row 수, -1이면 skip)
    """
    total = len(csv_files)
    loaded = 0
    skipped = 0
    failed = 0

    for i, csv_path in enumerate(csv_files, 1):
        sqlname = extract_sqlname_from_csv(csv_path)
        sql_file = sql_map.get(sqlname)

        if not sql_file:
            logger.warning("CSV[%d/%d] skip (sql not found): %s", i, total, csv_path.name)
            skipped += 1
            continue

        table_name = resolve_table_name(sql_file)
        file_hash = _sha256_file(csv_path)

        logger.info("LOAD [%d/%d] | table=%s | file=%s", i, total, table_name, csv_path.name)

        try:
            result = load_fn(table_name, csv_path, file_hash)
            if result == -1:
                skipped += 1
            else:
                loaded += 1
        except Exception as e:
            logger.exception("LOAD failed | table=%s | file=%s | %s", table_name, csv_path.name, e)
            failed += 1

    logger.info("LOAD summary | loaded=%d skipped=%d failed=%d", loaded, skipped, failed)