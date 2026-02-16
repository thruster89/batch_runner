import time
import re
import shutil
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from v2.adapters.sources.oracle_client import init_oracle_client, get_oracle_conn
from v2.adapters.sources.vertica_client import get_vertica_conn
from v2.engine.path_utils import resolve_path
from v2.engine.sql_utils import sort_sql_files
from v2.engine.runtime_state import stop_event

def expand_range_value(value: str):
    """
    지원 형식

    202101:202112
    202101:202112~Q   (분기말)
    202101:202112~H   (반기말)
    202101:202112~Y   (연말)
    """

    raw = value.strip()

    # 옵션 분리 (~ 권장)
    if "~" in raw:
        range_part, opt = raw.split("~", 1)
        opt = opt.upper().strip()
    else:
        range_part = raw
        opt = None

    # range가 아니면 그대로 반환
    if ":" not in range_part:
        return [range_part]

    start, end = range_part.split(":", 1)

    def to_int_ym(s):
        return int(s[:4]) * 12 + int(s[4:6]) - 1

    def to_str_ym(n):
        y = n // 12
        m = n % 12 + 1
        return f"{y:04d}{m:02d}"

    s = to_int_ym(start)
    e = to_int_ym(end)

    result = []

    for i in range(s, e + 1):
        ym = to_str_ym(i)

        month = ym[4:6]

        # 분기말
        if opt == "Q" and month not in ("03", "06", "09", "12"):
            continue

        # 반기말
        if opt == "H" and month not in ("06", "12"):
            continue

        # 연말
        if opt == "Y" and month != "12":
            continue

        result.append(ym)

    return result


def expand_params(params: dict):
    from itertools import product
    import logging

    logger = logging.getLogger(__name__)

    multi_keys = []
    values = []

    for k, v in params.items():
        v_str = str(v).strip()

        multi_keys.append(k)

        # 범위 표현 처리
        if ":" in v_str:
            expanded = expand_range_value(v_str)
            logger.info("Param expand | %s -> %d values", v_str, len(expanded))
            values.append(expanded)

        # 콤마 분리 처리
        elif "," in v_str:
            split_vals = [x.strip() for x in v_str.split(",")]
            logger.info("Param expand | %s -> %d values", v_str, len(split_vals))
            values.append(split_vals)

        # 단일 값
        else:
            logger.info("Param expand | %s -> 1 value", v_str)
            values.append([v_str])

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
        backup_dir.glob(prefix + "*.csv*"),
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


def build_log_prefix(sql_file: Path, params: dict) -> str:
    if not params:
        return f"[{sql_file.stem}]"

    short = []
    for k in sorted(params.keys()):
        short.append(f"{k}={params[k]}")

    return f"[{sql_file.stem}|{' '.join(short)}]"


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

    fmt = export_cfg.get("format", "csv")
    compression = export_cfg.get("compression", "none")
    overwrite = export_cfg.get("overwrite", False)
    backup_keep = export_cfg.get("backup_keep", 10)
    parallel_workers = export_cfg.get("parallel_workers", 1)

    if fmt == "csv" and compression == "gzip":
        ext = "csv.gz"
    elif fmt == "csv":
        ext = "csv"
    elif fmt == "parquet":
        ext = "parquet"
    else:
        raise ValueError("Unsupported format")

    stall_seconds = 30 * 60

    def _export_one(sql_file, param_set, idx, total_sql, param_idx, total_param):
        local_conn = None
        prefix = build_log_prefix(sql_file, param_set)

        try:
            if source_type == "oracle":
                from v2.adapters.sources.oracle_source import export_sql_to_csv as _export

                oracle_cfg = env_cfg["sources"]["oracle"]
                host_cfg = oracle_cfg["hosts"].get(host_name)

                if not host_cfg:
                    raise RuntimeError(f"Oracle host not found: {host_name}")

                init_oracle_client(oracle_cfg)
                local_conn = get_oracle_conn(host_cfg)
                export_func = _export
                fetch_size = oracle_cfg.get("export", {}).get("fetch_size", 10000)

            elif source_type == "vertica":
                from v2.adapters.sources.vertica_source import export_sql_to_csv as _export

                vertica_cfg = env_cfg["sources"]["vertica"]
                host_cfg = vertica_cfg["hosts"].get(host_name)
                local_conn = get_vertica_conn(host_cfg)
                export_func = _export
                fetch_size = vertica_cfg.get("export", {}).get("fetch_size", 10000)

            else:
                raise ValueError(f"Unsupported source type: {source_type}")

            csv_name = build_csv_name(
                sqlname=sql_file.stem,
                host=host_name,
                params=param_set,
                ext=ext,
            )

            out_file = out_dir / csv_name
            tmp_file = out_file.with_suffix(out_file.suffix + ".tmp")

            if out_file.exists() and not tmp_file.exists():
                if overwrite:
                    backup_dir = out_dir / "_backup"
                    backup_existing_file(out_file, backup_dir, keep=backup_keep)
                else:
                    logger.info("%s skip (already completed): %s", prefix, out_file.name)
                    return

            logger.info(
                "%s EXPORT start [%d/%d] param[%d/%d]",
                prefix, idx, total_sql, param_idx, total_param
            )

            sql_text = sql_file.read_text(encoding="utf-8")
            rendered_sql = sanitize_sql(_render_sql(sql_text, param_set))

            start_time = time.time()

            rows = export_func(
                conn=local_conn,
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
                "%s EXPORT done rows=%d size=%.2fMB elapsed=%.2fs",
                prefix,
                rows,
                size_mb,
                elapsed
            )

        finally:
            if local_conn:
                try:
                    local_conn.close()
                except Exception:
                    pass

    logger.info("Parallel workers=%d", parallel_workers)

    tasks = []
    for idx, sql_file in enumerate(sql_files, 1):
        for param_idx, param_set in enumerate(param_sets, 1):
            if stop_event.is_set():
                logger.warning("EXPORT stage aborted by user")
                break
            tasks.append((sql_file, param_set, idx, len(sql_files), param_idx, len(param_sets)))

    if parallel_workers <= 1:
        for t in tasks:
            if stop_event.is_set():
                logger.warning("EXPORT stopped by user")
                break
            _export_one(*t)
    else:
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            futures = []

            for t in tasks:
                if stop_event.is_set():
                    logger.warning("EXPORT submission stopped by user")
                    break
                futures.append(executor.submit(_export_one, *t))

            for f in as_completed(futures):
                if stop_event.is_set():
                    logger.warning("Waiting running tasks to finish...")
                f.result()

    logger.debug("EXPORT stage end")
