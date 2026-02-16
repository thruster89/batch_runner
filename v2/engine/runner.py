# file: v2/engine/runner.py
import json
import argparse
import yaml
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from v2.engine.stage_registry import STAGE_REGISTRY
from v2.engine.runtime_state import stop_event
import signal

# ----------------------------
# Context
# ----------------------------
@dataclass
class RunContext:
    job_name: str
    run_id: str
    job_config: dict
    env_config: dict
    params: dict
    work_dir: Path
    mode: str
    logger: logging.Logger = field(repr=False)

# ----------------------------
# Logging
# ----------------------------
def setup_logging(log_dir: Path, debug: bool):
    
    log_dir.mkdir(parents=True, exist_ok=True)

    run_date = datetime.now().strftime("%Y%m%d")
    log_file = log_dir / f"run_{run_date}.log"

    level = logging.DEBUG if debug else logging.INFO

    if debug:
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    else:
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )

    root = logging.getLogger()
    root.setLevel(level)

    if root.handlers:
        root.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    logger = logging.getLogger("batch_runner_v2")
    logger.setLevel(level)
    logger.propagate = True

    return logger

# ----------------------------
# Run ID generator
# ----------------------------
def generate_run_id(base_dir: Path, job_name: str) -> str:
    """
    job_name_01, job_name_02 형태 run_id 생성
    """
    job_dir = base_dir / job_name
    job_dir.mkdir(parents=True, exist_ok=True)

    existing = []
    for p in job_dir.iterdir():
        if p.is_dir() and p.name.startswith(f"{job_name}_"):
            try:
                suffix = p.name.replace(f"{job_name}_", "")
                existing.append(int(suffix))
            except ValueError:
                pass

    next_no = max(existing) + 1 if existing else 1
    return f"{job_name}_{next_no:02d}"


def write_run_info(run_dir: Path, ctx: RunContext, start_time: str):
    run_dir.mkdir(parents=True, exist_ok=True)

    info = {
        "job_name": ctx.job_name,
        "run_id": ctx.run_id,
        "start_time": start_time,
        "mode": ctx.mode,
        "params": ctx.params,
        "host": ctx.job_config.get("source", {}).get("host"),
    }

    with open(run_dir / "run_info.json", "w", encoding="utf-8") as f:
        json.dump(info, f, indent=2, ensure_ascii=False)

# ----------------------------
# Mode parse
# ----------------------------
def _parse_mode(v_mode: str) -> str:
    s = (v_mode or "").strip().lower()

    alias = {
        "dryrun": "plan",
        "dry-run": "plan",
        "plan": "plan",

        "normal": "run",
        "run": "run",
        "all": "run",
        "execute": "run",

        "retry": "retry",
        "failed": "retry",
        "replay": "retry",
        "fail": "retry",
    }

    if s not in alias:
        raise argparse.ArgumentTypeError(f"Invalid --mode: {v_mode} (use Dryrun/Normal/Retry)")
    return alias[s]


def _mode_display(v_mode: str) -> str:
    mapping = {"plan": "Plan", "run": "Run", "retry": "Retry"}
    return mapping.get(v_mode, v_mode)

# ----------------------------
# CLI param parser
# ----------------------------
def parse_cli_params(param_list):
    result = {}
    if not param_list:
        return result

    for item in param_list:
        if "=" not in item:
            raise ValueError(f"Invalid --param format: {item}")

        k, v = item.split("=", 1)
        result[k.strip()] = v.strip()

    return result

# ----------------------------
# Loader
# ----------------------------
def load_job(job_path: Path) -> dict:
    with open(job_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_env(env_path: Path) -> dict:
    with open(env_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# ----------------------------
# Runner
# ----------------------------
def run_pipeline(ctx: RunContext):
    stages = ctx.job_config.get("pipeline", {}).get("stages", [])

    if not stages:
        ctx.logger.warning("No stages defined in pipeline")
        return

    total = len(stages)

    ctx.logger.info("")
    ctx.logger.info("=" * 60)
    ctx.logger.info(" PIPELINE START")
    ctx.logger.info("-" * 60)
    ctx.logger.info("Stages total=%d | %s", total, stages)
    ctx.logger.info("")

    for idx, stage_name in enumerate(stages, 1):
        if stop_event.is_set():
            ctx.logger.warning("Pipeline stopped before stage execution")
            break
        
        stage_func = STAGE_REGISTRY.get(stage_name)

        if not stage_func:
            ctx.logger.error("Unknown stage: %s", stage_name)
            raise ValueError(f"Unknown stage: {stage_name}")

        ctx.logger.info("[%d/%d] %s", idx, len(stages), stage_name.upper())
        ctx.logger.info("-" * 60)

        start = time.time()
        stage_func(ctx)
        elapsed = time.time() - start

        ctx.logger.info("-" * 60)
        ctx.logger.info("[%d/%d] %s DONE (%.2fs)", idx, len(stages), stage_name.upper(), elapsed)
        ctx.logger.info("")

        if stop_event.is_set():
            ctx.logger.warning("Pipeline stopped by user")
            break

    ctx.logger.info("============== PIPELINE FINISHED ==============")

# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True, help="Path to job.yml")
    parser.add_argument("--env", default="config/env.yml", help="Path to env.yml")
    parser.add_argument("--workdir", default=".", help="Working directory")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--mode",
        type=_parse_mode,
        default="dryrun",
        help="Execution mode (Dryrun/Normal/Retry)",
    )
    parser.add_argument(
        "--param",
        action="append",
        help="Override parameter (key=value)",
    )

    args = parser.parse_args()

    job_path = Path(args.job)
    env_path = Path(args.env)
    work_dir = Path(args.workdir)

    job_config = load_job(job_path)
    env_config = load_env(env_path)

    logger = setup_logging(work_dir / "logs", debug=args.debug)
    job_name = job_config.get("job_name", "unnamed_job")

    def _handle_sigint(sig, frame):
        logger.warning("STOP requested (Ctrl+C)")
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sigint)

    export_base = Path(job_config.get("export", {}).get("out_dir", "data/export"))
    run_id = generate_run_id(export_base, job_name)
    start_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 기본 params
    params = dict(job_config.get("params", {}))

    # CLI override merge
    cli_params = parse_cli_params(args.param)
    params.update(cli_params)

    ctx = RunContext(
        job_name=job_name,
        run_id=run_id,
        job_config=job_config,
        env_config=env_config,
        params=params,
        work_dir=work_dir,
        mode=args.mode,
        logger=logger,
    )

    logger.info("")
    logger.info("=" * 60)
    logger.info(" JOB START")
    logger.info("-" * 60)

    logger.info(" Job Name : %s", ctx.job_name)
    logger.info(" Run ID   : %s", ctx.run_id)
    logger.info(" Start    : %s", start_time_str)

    source_sel = ctx.job_config.get("source", {})
    logger.info(" Mode     : %s", _mode_display(ctx.mode))
    logger.info(" Source   : %s", source_sel.get("type", "oracle"))
    logger.info(" Host     : %s", source_sel.get("host", "(default)"))

    export_cfg = ctx.job_config.get("export", {})
    logger.info(" SQL Dir  : %s", export_cfg.get("sql_dir"))
    logger.info(" Out Dir  : %s", export_cfg.get("out_dir"))

    if ctx.params:
        param_str = ", ".join(f"{k}={v}" for k, v in ctx.params.items())
        logger.info(" Params   : %s", param_str)

    logger.info(" WORK Dir : %s", ctx.work_dir.resolve())

    log_file = None
    root_logger = logging.getLogger()

    for h in root_logger.handlers:
        if isinstance(h, logging.FileHandler):
            log_file = h.baseFilename
            break

    logger.info(" Log file : %s", log_file)
    run_dir = export_base / job_name / run_id
    write_run_info(run_dir, ctx, start_time_str)
    logger.info("=" * 60)
    logger.info("")

    run_pipeline(ctx)
    logger.info("Job finished")


if __name__ == "__main__":
    main()
