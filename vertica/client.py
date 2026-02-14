import logging
import vertica_python

logger = logging.getLogger(__name__)


def init_vertica_client(source_cfg):
    """
    Vertica는 별도 client init 필요 없음
    인터페이스 통일 목적 함수
    """
    logger.info("Vertica client initialized")


def get_vertica_conn(host_cfg):
    """
    host_cfg 예:
      host:
      port:
      database:
      user:
      password:
    """

    conn_info = {
        "host": host_cfg["host"],
        "port": host_cfg.get("port", 5433),
        "database": host_cfg["database"],
        "user": host_cfg["user"],
        "password": host_cfg["password"],
        "autocommit": True,
    }

    return vertica_python.connect(**conn_info)
