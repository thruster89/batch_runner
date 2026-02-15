# file: v2/adapters/sources/vertica_client.py

import logging
import vertica_python


logger = logging.getLogger(__name__)


def get_vertica_conn(host_cfg: dict):
    """
    Vertica connection 생성
    """

    conn_info = {
        "host": host_cfg["host"],
        "port": host_cfg.get("port", 5433),
        "user": host_cfg["user"],
        "password": host_cfg.get("password", ""),
        "database": host_cfg["database"],
        "autocommit": True,
        "tlsmode": host_cfg.get("tlsmode", "disable"),
    }

    logger.info("Vertica connecting | %s:%s/%s",
                conn_info["host"], conn_info["port"], conn_info["database"])

    conn = vertica_python.connect(**conn_info)

    logger.info("Vertica connection established")

    return conn
