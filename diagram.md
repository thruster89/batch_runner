```mermaid
flowchart TD
    CLI["CLI<br/>batch_runner.py"] --> ARGS["parse_args / params"]

    ARGS -->|mode=DRYRUN| DRYRUN["DRYRUN<br/>sql + params 검증"]
    ARGS -->|mode=ALL| ALL["ALL 실행"]

    ALL --> ORACLE["Oracle Export"]
    ORACLE -->|csv| CSV["CSV files"]
    ORACLE -->|parquet direct| PARQUET["Parquet files"]

    CSV --> CSV2PARQ["csv → parquet"]
    PARQUET --> DUCKLOAD["DuckDB Load"]
    CSV2PARQ --> DUCKLOAD

    DUCKLOAD --> UNION["Union Views"]
    UNION --> EXCEL["Excel Export"]
    EXCEL --> STATS["Stats / Logs"]
