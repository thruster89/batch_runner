import oracledb

def init_oracle_thick(env):
    lib = env["oracle"]["thick"]["instant_client"]
    oracledb.init_oracle_client(lib_dir=lib)

    oracledb.defaults.arraysize = 10_000
    oracledb.defaults.prefetchrows = 10_000
    oracledb.defaults.call_timeout = 30 * 60 * 1000

def get_oracle_conn(host_cfg):
    return oracledb.connect(
        user=host_cfg["user"],
        password=host_cfg["password"],
        dsn=host_cfg["dsn"],
    )
