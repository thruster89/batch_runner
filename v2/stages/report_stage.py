# file: v2/stages/report_stage.py

from pathlib import Path


def run(ctx):
    logger = ctx.logger

    report_cfg = ctx.job_config.get("report")
    if not report_cfg:
        logger.info("REPORT stage skipped (no config)")
        return

    logger.info("REPORT stage start")

    # -----------------------------
    # CSV Export (골격)
    # -----------------------------
    export_csv_cfg = report_cfg.get("export_csv", {})
    if export_csv_cfg.get("enabled", False):

        sql_dir = export_csv_cfg.get("sql_dir")
        out_dir = export_csv_cfg.get("out_dir")

        logger.info(
            "REPORT csv export | sql_dir=%s out_dir=%s",
            sql_dir,
            out_dir,
        )

        # TODO:
        # 1. sql_dir 내 SQL 순차 실행
        # 2. 결과를 CSV로 저장
        # 3. ctx.params 치환 적용

    else:
        logger.info("REPORT csv export skipped")

    # -----------------------------
    # Excel Reporting (골격)
    # -----------------------------
    excel_cfg = report_cfg.get("excel", {})
    if excel_cfg.get("enabled", False):

        output_xlsx = excel_cfg.get("output")

        logger.info(
            "REPORT excel export | output=%s",
            output_xlsx,
        )

        # TODO:
        # 1. CSV merge
        # 2. 시트 생성
        # 3. 저장

    else:
        logger.info("REPORT excel export skipped")

    logger.info("REPORT stage end")
