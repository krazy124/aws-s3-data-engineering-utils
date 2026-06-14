"""MonsterForge Industries project-specific ETL pipeline."""

import logging
from io import StringIO

import boto3
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when

from aws_clients import run_safely
from s3_operations import write_dataframe_to_s3_parquet
from glue_transformations import (
    log_step,
    print_quality_report,
    clean_column_names,
    trim_string_columns,
    add_missing_flag,
    standardize_category_column,
    parse_currency_to_double,
    fix_negative_values_with_flag,
    parse_multiple_date_formats,
    convert_to_integer,
    select_columns,
    validate_required_columns,
    normalize_empty_strings_to_null,
    add_ingestion_timestamp,
    add_source_file_column,
    generate_null_count_report,
    generate_duplicate_report,
)

logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")


def monsterforge_cleaning_pipeline(df: DataFrame):
    """MonsterForge-specific cleaning pipeline for monsters.csv."""

    monster_type_mapping = {
        "zombie": "zombie",
        "undead": "zombie",
        "vampire": "vampire",
        "dragon": "dragon",
        "beast": "beast",
        "ice beast": "beast",
        "ghost": "ghost",
    }

    status_mapping = {
        "active": "active",
        "inactive": "inactive",
        "testing": "testing",
        "unknown": "unknown",
        "retired": "retired",
    }

    df = clean_column_names(df)

    required_columns = [
        "monster_id",
        "monster_name",
        "monster_type",
        "base_price",
        "created_date",
        "danger_level",
        "status",
    ]

    validation_report = validate_required_columns(df, required_columns)
    if not validation_report["is_valid"]:
        raise ValueError(
            f"Missing required MonsterForge columns: {validation_report['missing_columns']}"
        )

    df = trim_string_columns(df)
    df = normalize_empty_strings_to_null(df)
    df = add_ingestion_timestamp(df)
    df = add_source_file_column(df)

    df = add_missing_flag(df, "monster_id")
    df = add_missing_flag(df, "monster_name")
    df = add_missing_flag(df, "created_date")

    df = standardize_category_column(df, "monster_type", monster_type_mapping)
    df = standardize_category_column(df, "status", status_mapping)

    df = parse_currency_to_double(df, "base_price")
    df = fix_negative_values_with_flag(df, "base_price")

    df = parse_multiple_date_formats(df, "created_date")
    df = convert_to_integer(df, "danger_level")

    expected_columns = [
        "monster_id",
        "monster_name",
        "monster_type",
        "danger_level",
        "created_date",
        "base_price",
        "status",
        "monster_id_missing_flag",
        "monster_name_missing_flag",
        "created_date_missing_flag",
        "base_price_corrected_flag",
        "ingestion_timestamp",
        "source_file",
    ]

    return select_columns(df, expected_columns)


def split_clean_quarantine(df: DataFrame):
    """Split cleaned MonsterForge records into clean and quarantine DataFrames."""

    quarantine_condition = (
        col("monster_id_missing_flag")
        | col("monster_name_missing_flag")
        | col("created_date").isNull()
        | col("danger_level").isNull()
    )

    quarantine_df = df.withColumn(
        "quarantine_reason",
        when(col("monster_id_missing_flag"), "missing_monster_id")
        .when(col("monster_name_missing_flag"), "missing_monster_name")
        .when(col("created_date").isNull(), "invalid_created_date")
        .when(col("danger_level").isNull(), "invalid_danger_level")
        .otherwise("unknown_reason"),
    ).filter(quarantine_condition)

    clean_df = df.filter(~quarantine_condition)

    return clean_df, quarantine_df


def load_monsterforge_raw_data(spark: SparkSession, input_path: str):
    """Load raw MonsterForge CSV data."""
    log_step("Loading Raw MonsterForge Data")

    def load_action():
        return spark.read.option("header", True).csv(input_path)

    return run_safely(
        load_action,
        default_return=None,
        error_message=f"Failed to load raw data from {input_path}",
    )


def generate_quality_report(df_cleaned, clean_df, quarantine_df):
    """Generate MonsterForge data quality metrics."""
    null_counts = generate_null_count_report(df_cleaned)
    duplicate_report = generate_duplicate_report(df_cleaned, ["monster_id"])

    return {
        "total_records": df_cleaned.count(),
        "clean_records": clean_df.count(),
        "quarantined_records": quarantine_df.count(),
        "missing_ids": df_cleaned.filter(col("monster_id_missing_flag")).count(),
        "missing_names": df_cleaned.filter(col("monster_name_missing_flag")).count(),
        "invalid_dates": df_cleaned.filter(col("created_date").isNull()).count(),
        "invalid_danger_levels": df_cleaned.filter(col("danger_level").isNull()).count(),
        "corrected_negative_prices": df_cleaned.filter(
            col("base_price_corrected_flag")
        ).count(),
        "duplicate_monster_ids": duplicate_report["duplicate_rows"],
        "null_counts": null_counts,
    }


def upload_pandas_csv_to_s3(pandas_df, bucket_name: str, s3_key: str):
    """Upload a Pandas DataFrame to S3 as a CSV file."""

    def upload_action():
        csv_buffer = StringIO()
        pandas_df.to_csv(csv_buffer, index=False)

        s3_client = boto3.client("s3")

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
        )

        print(f"Written to: s3://{bucket_name}/{s3_key}")
        return True

    return run_safely(
        upload_action,
        default_return=False,
        error_message=f"Failed to upload s3://{bucket_name}/{s3_key}",
    )


def write_clean_data_to_s3(clean_df, bucket_name: str):
    """Write clean MonsterForge records to S3 as CSV."""
    log_step("Writing Clean Data To S3")

    pandas_df = clean_df.toPandas()

    return upload_pandas_csv_to_s3(
        pandas_df=pandas_df,
        bucket_name=bucket_name,
        s3_key="clean/monsters/monsters_clean.csv",
    )


def write_clean_data_to_s3_parquet(clean_df, bucket_name: str):
    """Write clean MonsterForge records to S3 as Parquet partitioned by status."""
    log_step("Writing Clean Data To S3 As Parquet")

    return write_dataframe_to_s3_parquet(
        df=clean_df,
        bucket_name=bucket_name,
        object_prefix="clean_parquet/monsters/",
        mode="overwrite",
        partition_by=["status"],
    )


def write_quarantine_data_to_s3(quarantine_df, bucket_name: str):
    """Write quarantined MonsterForge records to S3 as CSV."""
    log_step("Writing Quarantine Data To S3")

    pandas_df = quarantine_df.toPandas()

    return upload_pandas_csv_to_s3(
        pandas_df=pandas_df,
        bucket_name=bucket_name,
        s3_key="quarantine/monsters/monsters_quarantine.csv",
    )


def write_quality_report_to_s3(report: dict, bucket_name: str):
    """Write MonsterForge quality report to S3 as CSV."""
    log_step("Writing Quality Report To S3")

    report_rows = [
        {"metric": metric, "value": value}
        for metric, value in report.items()
    ]

    pandas_df = pd.DataFrame(report_rows)

    return upload_pandas_csv_to_s3(
        pandas_df=pandas_df,
        bucket_name=bucket_name,
        s3_key="reports/monsterforge_quality_report.csv",
    )


def run_monsterforge_etl(spark: SparkSession, input_path: str, bucket_name: str = None, preview: bool = True, ):
    """Run the full MonsterForge transformation, quarantine, reporting, and optional S3 output flow."""

    try:
        df_raw = load_monsterforge_raw_data(spark, input_path)

        if df_raw is None:
            raise RuntimeError("Raw data load failed. ETL stopped.")

        if preview:
            log_step("Raw Data Preview")
            df_raw.select(
                "Monster ID",
                "Base Price",
                "Created Date",
                "Danger Level",
            ).show(10, False)

        log_step("Applying MonsterForge Cleaning Pipeline")
        df_cleaned = monsterforge_cleaning_pipeline(df_raw)

        if preview:
            log_step("Cleaned Data Preview")
            df_cleaned.select(
                "monster_id",
                "base_price",
                "created_date",
                "danger_level",
                "base_price_corrected_flag",
            ).show(10, False)

        log_step("Splitting Clean and Quarantine Records")
        clean_df, quarantine_df = split_clean_quarantine(df_cleaned)

        if preview:
            log_step("Quarantined Records Preview")
            quarantine_df.select(
                "monster_id",
                "monster_name",
                "created_date",
                "danger_level",
                "quarantine_reason",
            ).show(50, False)

        log_step("Generating Data Quality Report")
        report = generate_quality_report(df_cleaned, clean_df, quarantine_df)
        print_quality_report(report)

        upload_results = {}

        if bucket_name:
            upload_results["clean_data"] = write_clean_data_to_s3(
                clean_df,
                bucket_name,
            )

            upload_results["quarantine_data"] = write_quarantine_data_to_s3(
                quarantine_df,
                bucket_name,
            )

            upload_results["quality_report"] = write_quality_report_to_s3(
                report,
                bucket_name,
            )

            upload_results["clean_parquet"] = bool(write_clean_data_to_s3_parquet(
                clean_df,
                bucket_name,
            ))

            failed_uploads = [
                name
                for name, success in upload_results.items()
                if not success
            ]

            if failed_uploads:
                logging.warning(f"Some S3 uploads failed: {failed_uploads}")
            else:
                log_step("All S3 Outputs Written Successfully")

        return clean_df, quarantine_df, report

    except Exception:
        logging.exception("MonsterForge ETL failed.")
        raise
