# core/args.py
import argparse

def parse_args():
    parser = argparse.ArgumentParser("Oracle → CSV → DuckDB Batch Runner")

    parser.add_argument("--mode", type=str.upper, choices=["DRYRUN", "ALL", "RETRY"], default="DRYRUN")
    parser.add_argument("--source", choices=["oracle", "vertica"], default="oracle", help="Source database type")
    
    parser.add_argument("--hosts", help="Comma-separated host list (override env.yml)")
    parser.add_argument("--params", help="Comma-separated params, e.g. clsYymm=202501,fromYymm=202401")

    parser.add_argument(
        "--param",
        action="append",
        help="Parameter override, e.g. --param clsYymm=202312,202401",
    )

    parser.add_argument("--duckdb-file", help="DuckDB file path or name. If relative, saved under ./duckdb/")

    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Intermediate file format (csv or parquet)")

    parser.add_argument(
    "--sql-filter",
    help="Run only SQL files whose path contains this string (comma separated)"
)

    parser.add_argument("--no-excel", action="store_true", help="Skip Excel export")
    parser.add_argument(
        "--sql-subdirs",
        help=(
            "Comma-separated SQL subdirectories under sql/<source>/<host>/<subdir>. "
            "Example: A or A,B or A/risk"
        ),
    )
    parser.add_argument(
        "--duckdb-sql-dir",
        help="Directory containing DuckDB postwork SQL files",
    )

    parser.add_argument(
        "--skip-export",
        action="store_true",
        help="Skip export step (use existing CSV/Parquet)",
    )

    parser.add_argument(
        "--skip-duckdb-sql",
        action="store_true",
        help="Skip DuckDB postwork SQL execution",
    )
    
    parser.add_argument(
    "--duckdb-sql-filter",
    help="Run only matching DuckDB SQL files (comma separated)",
)

    
    return parser.parse_args()


# def parse_params_override(param_str: str) -> dict:
#     """
#     안전 파서 (최종본)

#     허용 형식:
#       --params k=v1,v2,v3;k2=v4,v5

#     규칙:
#     - 파라미터 간 구분자: ;
#     - 값 리스트 구분자: ,
#     """
#     result = {}

#     if not param_str:
#         return result

#     pairs = param_str.split(";")

#     for pair in pairs:
#         pair = pair.strip()
#         if not pair:
#             continue

#         if "=" not in pair:
#             raise ValueError(
#                 f"Invalid --params token (expected k=v[,v]): {pair}"
#             )

#         k, v = pair.split("=", 1)
#         k = k.strip()
#         v = v.strip()

#         if not k or not v:
#             raise ValueError(f"Invalid --params token: {pair}")

#         # 값은 그대로 문자열 유지 (뒤에서 expand_param_value가 처리)
#         result[k] = v

#     return result


def parse_params_override(param_list: list[str] | None) -> dict:
    result = {}

    if not param_list:
        return result

    for pair in param_list:
        if "=" not in pair:
            raise ValueError(f"Invalid --param: {pair}")

        k, v = pair.split("=", 1)
        result[k.strip()] = v.strip()

    return result