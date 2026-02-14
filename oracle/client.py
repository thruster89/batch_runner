import logging
import oracledb

logger = logging.getLogger(__name__)


def init_oracle_client(source_cfg):
    """
    Thick mode 시도 → 실패 시 Thin fallback
    기존 init_oracle_thick 역할 포함
    """
    lib = source_cfg.get("thick", {}).get("instant_client")

    mode = "thin"

    if lib:
        try:
            oracledb.init_oracle_client(lib_dir=lib)
            logger.info("Oracle Thick mode initialized: %s", lib)
            mode = "thick"
        except Exception as e:
            logger.warning("Thick mode init failed → fallback to Thin mode")
            logger.warning("Reason: %s", e)
    else:
        logger.info("Instant client not configured → using Thin mode")

    # 공통 성능 설정 (thin/thick 동일 적용 가능)
    oracledb.defaults.arraysize = 10_000
    oracledb.defaults.prefetchrows = 10_000
    oracledb.defaults.call_timeout = 30 * 60 * 1000
    logger.info("Oracle client mode: %s", mode)
    return mode


def get_oracle_conn(host_cfg):
    return oracledb.connect(
        user=host_cfg["user"],
        password=host_cfg["password"],
        dsn=host_cfg["dsn"],
    )


# 기존 thick 전용 init_oracle_thick 함수는 init_oracle_client로 통합했음

# def init_oracle_thick(env):
#     lib = env["oracle"]["thick"]["instant_client"] 
    
#     oracledb.init_oracle_client(lib_dir=lib)
    
#     oracledb.defaults.arraysize = 10_000
#     oracledb.defaults.prefetchrows = 10_000
#     oracledb.defaults.call_timeout = 30 * 60 * 1000 