import logging
import time
from util.paths import LOG_DIR

def setup_logging(batch_date: str):
    LOG_DIR.mkdir(exist_ok=True)

    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    fh_app = logging.FileHandler(LOG_DIR / f"app_{batch_date}.log", encoding="utf-8")
    fh_app.setLevel(logging.INFO)
    fh_app.setFormatter(fmt)
    root.addHandler(fh_app)

    fh_debug = logging.FileHandler(LOG_DIR / f"debug_{batch_date}.log", encoding="utf-8")
    fh_debug.setLevel(logging.DEBUG)
    fh_debug.setFormatter(fmt)
    root.addHandler(fh_debug)

def cleanup_old_logs(retention_days=365):
    cutoff = time.time() - retention_days * 86400
    for f in LOG_DIR.glob("*.log"):
        if f.stat().st_mtime < cutoff:
            f.unlink()

def get_host_logger(host_name: str, batch_date: str) -> logging.Logger:
    logger = logging.getLogger(f"host.{host_name}")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")

    fh = logging.FileHandler(
        LOG_DIR / f"host_{host_name}_{batch_date}.log",
        encoding="utf-8",
    )
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)

    logger.addHandler(fh)
    return logger
