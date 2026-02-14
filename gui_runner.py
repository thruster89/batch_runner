import tkinter as tk
from tkinter import ttk, filedialog
from tkinter.scrolledtext import ScrolledText
from pathlib import Path
import subprocess
import threading
import yaml
import json
import re

# =========================================================
# FILES
# =========================================================
CONFIG_FILE = Path("config/env.yml")
LAST_USED_FILE = Path("gui_last_used.json")

# =========================================================
# PARAM PATTERNS (3 types mixed)
#   :param
#   {#param}
#   ${param}
# =========================================================
PARAM_PATTERNS = [
    re.compile(r":([A-Za-z_][A-Za-z0-9_]*)"),
    re.compile(r"\{\#([A-Za-z_][A-Za-z0-9_]*)\}"),
    re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}"),
]

# =========================================================
# last used
# =========================================================
def load_last_used() -> dict:
    if LAST_USED_FILE.exists():
        try:
            return json.loads(LAST_USED_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_last_used(d: dict) -> None:
    try:
        LAST_USED_FILE.write_text(json.dumps(d, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

# =========================================================
# config
# =========================================================
def load_env() -> dict:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_FILE.as_posix()}")
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

env_data = load_env()
if "sources" not in env_data:
    raise KeyError("env.yml must have top-level key: sources")

last_used = load_last_used()

# =========================================================
# SQL param scan
# =========================================================
def scan_params_in_subdirs(sql_base: Path, subdirs: list[str]) -> list[str]:
    found = set()

    for sub in subdirs:
        sub_path = sql_base / sub
        if not sub_path.exists():
            continue

        for sql_file in sub_path.rglob("*.sql"):
            try:
                text = sql_file.read_text(encoding="utf-8", errors="ignore")
                for pat in PARAM_PATTERNS:
                    for m in pat.findall(text):
                        if m:
                            found.add(m)
            except Exception:
                # UI 안정성 우선: 실패는 무시
                pass

    return sorted(found)

# =========================================================
# GUI
# =========================================================
root = tk.Tk()
root.title("Batch Runner GUI")
root.geometry("1440x960")

# ---- main split 50:50
main = ttk.Frame(root, padding=10)
main.pack(fill="both", expand=True)

main.columnconfigure(0, weight=1)
main.columnconfigure(1, weight=1)
main.rowconfigure(0, weight=1)

left = ttk.Frame(main)
left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))

right = ttk.Frame(main)
right.grid(row=0, column=1, sticky="nsew", padx=(8, 0))

# =========================================================
# LEFT: Controls (grid only, no pack mix)
# =========================================================
left.columnconfigure(0, weight=1)

r = 0

ttk.Label(left, text="Source").grid(row=r, column=0, sticky="w"); r += 1
source_var = tk.StringVar(value="oracle")
source_box = ttk.Combobox(left, textvariable=source_var, values=["oracle", "vertica"], state="readonly")
source_box.grid(row=r, column=0, sticky="ew"); r += 1

ttk.Label(left, text="Mode").grid(row=r, column=0, sticky="w", pady=(8, 0)); r += 1
mode_var = tk.StringVar(value="ALL")
mode_box = ttk.Combobox(left, textvariable=mode_var, values=["DRYRUN", "ALL", "RETRY"], state="readonly")
mode_box.grid(row=r, column=0, sticky="ew"); r += 1

ttk.Label(left, text="Format").grid(row=r, column=0, sticky="w", pady=(8, 0)); r += 1
format_var = tk.StringVar(value="csv")
format_box = ttk.Combobox(left, textvariable=format_var, values=["csv", "parquet"], state="readonly")
format_box.grid(row=r, column=0, sticky="ew"); r += 1

# ---- Hosts
ttk.Label(left, text="Hosts (click -> show subdirs)").grid(row=r, column=0, sticky="w", pady=(10, 0)); r += 1
host_list = tk.Listbox(left, height=6, exportselection=False, selectmode="browse")
host_list.grid(row=r, column=0, sticky="ew"); r += 1

# ---- SQL Subdirs
ttk.Label(left, text="SQL Subdirs (click -> scan params)").grid(row=r, column=0, sticky="w", pady=(10, 0)); r += 1
sql_list = tk.Listbox(left, height=7, exportselection=False, selectmode="extended")
sql_list.grid(row=r, column=0, sticky="ew"); r += 1

# ---- Params detected
ttk.Label(left, text="Detected Params (:p / {#p} / ${p})").grid(row=r, column=0, sticky="w", pady=(10, 0)); r += 1
param_list = tk.Listbox(left, height=6, exportselection=False)
param_list.grid(row=r, column=0, sticky="ew"); r += 1

# ---- Params input (used to build --param)
ttk.Label(left, text="Params Input (space separated)  ex) clsYymm=202301:202312 exeIdno=221").grid(
    row=r, column=0, sticky="w", pady=(10, 0)
); r += 1

params_entry = ttk.Entry(left)
params_entry.grid(row=r, column=0, sticky="ew"); r += 1

# ---- DuckDB file + browse
ttk.Label(left, text="DuckDB File").grid(row=r, column=0, sticky="w", pady=(10, 0)); r += 1
duckdb_file_var = tk.StringVar(value=last_used.get("duckdb_file", "example.duckdb"))
duckdb_entry = ttk.Entry(left, textvariable=duckdb_file_var)
duckdb_entry.grid(row=r, column=0, sticky="ew"); r += 1

def browse_duckdb_file():
    f = filedialog.asksaveasfilename(defaultextension=".duckdb", filetypes=[("DuckDB", "*.duckdb"), ("All", "*.*")])
    if f:
        duckdb_file_var.set(f)
        update_cli_preview()

ttk.Button(left, text="Browse DuckDB File", command=browse_duckdb_file).grid(row=r, column=0, sticky="w"); r += 1

# ---- DuckDB SQL dir + browse
ttk.Label(left, text="DuckDB SQL Dir").grid(row=r, column=0, sticky="w", pady=(10, 0)); r += 1
duckdb_sql_dir_var = tk.StringVar(value=last_used.get("duckdb_sql_dir", "duckdb_sql"))
duckdb_sql_entry = ttk.Entry(left, textvariable=duckdb_sql_dir_var)
duckdb_sql_entry.grid(row=r, column=0, sticky="ew"); r += 1

def browse_duckdb_sql_dir():
    d = filedialog.askdirectory()
    if d:
        duckdb_sql_dir_var.set(d)
        update_cli_preview()

ttk.Button(left, text="Browse DuckDB SQL Dir", command=browse_duckdb_sql_dir).grid(row=r, column=0, sticky="w"); r += 1

# ---- flags
skip_export_var = tk.BooleanVar(value=False)
skip_duckdb_sql_var = tk.BooleanVar(value=False)
no_excel_var = tk.BooleanVar(value=False)

ttk.Checkbutton(left, text="Skip Export (--skip-export)", variable=skip_export_var, command=lambda: update_cli_preview())\
   .grid(row=r, column=0, sticky="w", pady=(10, 0)); r += 1
ttk.Checkbutton(left, text="Skip DuckDB SQL (--skip-duckdb-sql)", variable=skip_duckdb_sql_var, command=lambda: update_cli_preview())\
   .grid(row=r, column=0, sticky="w"); r += 1

no_excel_chk = ttk.Checkbutton(
    left,
    text="No Excel (--no-excel)",
    variable=no_excel_var,
    command=lambda: update_cli_preview()
)
no_excel_chk.grid(row=r, column=0, sticky="w"); r += 1

# ---- CLI Preview (real-time)
ttk.Label(left, text="CLI Preview (auto)").grid(row=r, column=0, sticky="w", pady=(12, 0)); r += 1
cli_preview = ScrolledText(left, height=8, font=("Consolas", 10))
cli_preview.grid(row=r, column=0, sticky="nsew"); r += 1

# left frame stretch for preview
left.rowconfigure(r-1, weight=1)

# ---- Run button
def run_batch():
    cmd = build_cmd_list()
    # save last used
    save_last_used({
        "duckdb_file": duckdb_file_var.get().strip(),
        "duckdb_sql_dir": duckdb_sql_dir_var.get().strip(),
    })

    console_write(f"\n[RUN] {' '.join(cmd)}\n")

    def worker():
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            for line in p.stdout:
                console_write(line)
            rc = p.wait()
            console_write(f"\n[EXIT] code={rc}\n")
        except Exception as e:
            console_write(f"\n[ERROR] {e}\n")

    threading.Thread(target=worker, daemon=True).start()

ttk.Button(left, text="Run", command=run_batch).grid(row=r, column=0, sticky="ew", pady=(10, 0)); r += 1

# =========================================================
# RIGHT: Console
# =========================================================
right.rowconfigure(0, weight=1)
right.columnconfigure(0, weight=1)

console = ScrolledText(
    right,
    bg="#252526",
    fg="#dcdcdc",
    insertbackground="white",
    font=("Consolas", 11),
)
console.grid(row=0, column=0, sticky="nsew")

def console_write(msg: str):
    # UI thread-safe enough for this scale; if needed, queue로 바꿀 수 있음
    console.insert(tk.END, msg)
    console.see(tk.END)

# =========================================================
# Build command + CLI preview
# =========================================================
def parse_param_pairs(text: str) -> list[str]:
    """
    GUI 입력: "clsYymm=202301:202312;region=KR"
    -> ["clsYymm=202301:202312", "region=KR"]
    """
    t = (text or "").strip()
    if not t:
        return []
    parts = [p.strip() for p in t.split() if p.strip()]
    return parts

def build_cmd_list() -> list[str]:
    cmd = ["python", "batch_runner.py"]

    cmd += ["--source", source_var.get().strip()]
    cmd += ["--mode", mode_var.get().strip()]
    cmd += ["--format", format_var.get().strip()]

    # host (single selection)
    if host_list.curselection():
        host = host_list.get(host_list.curselection()[0])
        cmd += ["--hosts", host]

    # subdirs (multi)
    if sql_list.curselection():
        subs = ",".join(sql_list.get(i) for i in sql_list.curselection())
        cmd += ["--sql-subdirs", subs]

    # params: multiple --param
    pairs = parse_param_pairs(params_entry.get())
    for pair in pairs:
        cmd += ["--param", pair]

    # duckdb file / dir
    if duckdb_file_var.get().strip():
        cmd += ["--duckdb-file", duckdb_file_var.get().strip()]
    if duckdb_sql_dir_var.get().strip():
        cmd += ["--duckdb-sql-dir", duckdb_sql_dir_var.get().strip()]

    # flags
    if skip_export_var.get():
        cmd.append("--skip-export")
    if skip_duckdb_sql_var.get():
        cmd.append("--skip-duckdb-sql")
    if no_excel_var.get():
        cmd.append("--no-excel")
    return cmd

def build_cli_preview_text(cmd: list[str]) -> str:
    """
    보기 좋게 ^ 줄바꿈
    """
    out = []
    # 첫 2개는 "python batch_runner.py"로 묶기
    if len(cmd) >= 2:
        out.append(f"{cmd[0]} {cmd[1]}")
        i = 2
    else:
        out.append(" ".join(cmd))
        i = len(cmd)

    while i < len(cmd):
        token = cmd[i]
        if token.startswith("--"):
            # 옵션은 다음 값이 있을 수도 있음
            if i + 1 < len(cmd) and not cmd[i + 1].startswith("--"):
                out.append(f"^ {token} {cmd[i+1]}")
                i += 2
            else:
                out.append(f"^ {token}")
                i += 1
        else:
            out.append(f"^ {token}")
            i += 1

    return "\n".join(out)

def update_cli_preview(*_):
    cmd = build_cmd_list()
    txt = build_cli_preview_text(cmd)
    cli_preview.delete("1.0", tk.END)
    cli_preview.insert(tk.END, txt)

# =========================================================
# Data refresh flows
#   source change -> hosts reload -> clear subdirs/params
#   host click    -> subdirs reload -> clear params
#   subdir click  -> scan params
# =========================================================
def refresh_hosts():
    host_list.delete(0, tk.END)
    sql_list.delete(0, tk.END)
    param_list.delete(0, tk.END)

    src = source_var.get().strip()
    cfg = env_data.get("sources", {}).get(src, {})
    hosts = (cfg.get("hosts") or {}).keys()

    for h in hosts:
        host_list.insert(tk.END, h)

    update_cli_preview()

def refresh_sql_subdirs(event=None):
    sql_list.delete(0, tk.END)
    param_list.delete(0, tk.END)

    if not host_list.curselection():
        update_cli_preview()
        return

    host = host_list.get(host_list.curselection()[0])
    src = source_var.get().strip()

    sql_base = Path("sql") / src / host
    if not sql_base.exists():
        update_cli_preview()
        return

    for p in sorted(sql_base.iterdir()):
        if p.is_dir():
            sql_list.insert(tk.END, p.name)

    update_cli_preview()

def refresh_params(event=None):
    param_list.delete(0, tk.END)

    if not host_list.curselection():
        update_cli_preview()
        return
    if not sql_list.curselection():
        update_cli_preview()
        return

    host = host_list.get(host_list.curselection()[0])
    src = source_var.get().strip()
    subdirs = [sql_list.get(i) for i in sql_list.curselection()]

    sql_base = Path("sql") / src / host
    params = scan_params_in_subdirs(sql_base, subdirs)

    for p in params:
        param_list.insert(tk.END, p)

    update_cli_preview()

def on_param_double_click(event=None):
    # 더블클릭한 param을 params_entry에 "param=" 형태로 붙이기
    sel = param_list.curselection()
    if not sel:
        return
    name = param_list.get(sel[0]).strip()
    if not name:
        return

    cur = params_entry.get().strip()
    # 이미 있으면 추가하지 않음
    if name in cur:
        return

    if cur:
        params_entry.insert(tk.END, f" {name}=")
    else:
        params_entry.insert(tk.END, f"{name}=")

    update_cli_preview()

# =========================================================
# bindings (IMPORTANT)
# =========================================================
source_box.bind("<<ComboboxSelected>>", lambda e: refresh_hosts())
mode_box.bind("<<ComboboxSelected>>", lambda e: update_cli_preview())
# format_box.bind("<<ComboboxSelected>>", lambda e: update_cli_preview())
def on_format_change(event=None):
    if format_var.get() == "parquet":
        no_excel_var.set(True)
        no_excel_chk.state(["disabled"])
    else:
        no_excel_chk.state(["!disabled"])
    update_cli_preview()

format_box.bind("<<ComboboxSelected>>", on_format_change)

host_list.bind("<<ListboxSelect>>", refresh_sql_subdirs)
sql_list.bind("<<ListboxSelect>>", refresh_params)

param_list.bind("<Double-Button-1>", on_param_double_click)

# real-time preview on typing
params_entry.bind("<KeyRelease>", lambda e: update_cli_preview())
duckdb_entry.bind("<KeyRelease>", lambda e: update_cli_preview())
duckdb_sql_entry.bind("<KeyRelease>", lambda e: update_cli_preview())

# =========================================================
# init
# =========================================================
refresh_hosts()
update_cli_preview()

root.mainloop()
