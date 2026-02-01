# generate_ps1.py

from pathlib import Path
import yaml

# =========================================================
# PATH
# =========================================================

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = BASE_DIR / "config"
PS_DIR = BASE_DIR / "ps"

ENV_YML = CONFIG_DIR / "env.yml"
PARAM_YML = CONFIG_DIR / "params.yml"

# =========================================================
# UTIL
# =========================================================

def load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# =========================================================
# PS OPTIONS
# =========================================================

PARQUET_OPT = " `\n  --export-parquet"

# =========================================================
# PS TEMPLATE
# =========================================================

PS_TEMPLATE = """Set-Location "{base_dir}"

if (Test-Path ".venv\\Scripts\\Activate.ps1") {{
    . .venv\\Scripts\\Activate.ps1
}} else {{
    Write-Error "venv not found"
    exit 1
}}

python batch_runner.py `
  --mode {mode} `
  --hosts {hosts}{params}{parquet}

if ($LASTEXITCODE -ne 0) {{
    Write-Error "Batch failed"
    exit $LASTEXITCODE
}}

Write-Host "Batch finished successfully"
"""

# =========================================================
# GENERATE
# =========================================================

def generate():
    env = load_yaml(ENV_YML)
    params = load_yaml(PARAM_YML)

    PS_DIR.mkdir(exist_ok=True)

    hosts = env["oracle"]["run"]["hosts"]

    # params 문자열 (ALL 전용)
    param_str = ""
    if params:
        kv = ",".join(f"{k}={v}" for k, v in params.items())
        param_str = f" `\n  --params {kv}"

    # -----------------------------
    # DRYRUN (전체 host)
    # -----------------------------
    dryrun_ps = PS_TEMPLATE.format(
        base_dir=BASE_DIR,
        mode="DRYRUN",
        hosts=",".join(hosts),
        params="",
        parquet="",
    )
    (PS_DIR / "run_DRYRUN_all_hosts.ps1").write_text(
        dryrun_ps, encoding="utf-8"
    )

    # -----------------------------
    # ALL / RETRY (host별)
    # -----------------------------
    for host in hosts:
        # ALL
        all_ps = PS_TEMPLATE.format(
            base_dir=BASE_DIR,
            mode="ALL",
            hosts=host,
            params=param_str,
            parquet=PARQUET_OPT,
        )
        (PS_DIR / f"run_ALL_{host}.ps1").write_text(
            all_ps, encoding="utf-8"
        )

        # RETRY
        retry_ps = PS_TEMPLATE.format(
            base_dir=BASE_DIR,
            mode="RETRY",
            hosts=host,
            params="",
            parquet="",
        )
        (PS_DIR / f"run_RETRY_{host}.ps1").write_text(
            retry_ps, encoding="utf-8"
        )

    print(f"[OK] .ps1 files generated in: {PS_DIR}")

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    generate()
