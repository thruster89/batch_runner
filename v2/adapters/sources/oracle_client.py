import logging
import threading
import oracledb

logger = logging.getLogger(__name__)

_oracle_client_initialized = False
_oracle_client_mode = None
_init_lock = threading.Lock()


def init_oracle_client(source_cfg):
    global _oracle_client_initialized
    global _oracle_client_mode

    if _oracle_client_initialized:
        return _oracle_client_mode

    with _init_lock:
        # double-check
        if _oracle_client_initialized:
            return _oracle_client_mode

        lib = source_cfg.get("thick", {}).get("instant_client")
        mode = "thin"

        if lib:
            try:
                oracledb.init_oracle_client(lib_dir=lib)
                mode = "thick"
                logger.info("Oracle client initialized (thick) | lib=%s", lib)
            except Exception as e:
                logger.warning("Oracle thick init failed → fallback to thin")
                logger.warning("Reason: %s", e)
        else:
            logger.info("Oracle client initialized (thin)")

        oracledb.defaults.arraysize = 10_000
        oracledb.defaults.prefetchrows = 10_000

        if hasattr(oracledb.defaults, "call_timeout"):
            setattr(oracledb.defaults, "call_timeout", 30 * 60 * 1000)

        logger.debug("Oracle thin mode: %s", oracledb.is_thin_mode())

        _oracle_client_initialized = True
        _oracle_client_mode = mode

        return mode


def get_oracle_conn(host_cfg):
    conn = oracledb.connect(
        user=host_cfg["user"],
        password=host_cfg["password"],
        dsn=host_cfg["dsn"],
    )

    logger.debug("Oracle connection opened | dsn=%s", host_cfg["dsn"])
    return conn
