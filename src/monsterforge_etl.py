# monsterforge_etl.py
# MonsterForge Industries ETL Pipeline
# Project-specific pipeline built on reusable PySpark transformation helpers

import logging
from io import StringIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when

from glue_transformations import (log_step, print_quality_report,
                                  clean_column_names, trim_string_columns,
                                  add_missing_flag, standardize_category_column,
                                  parse_currency_to_double, fix_negative_values_with_flag,
                                  parse_multiple_date_formats, convert_to_integer,
                                  select_columns, create_glue_crawler)


logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# MF00.v1 - Run Safely
def run_safely(action, default_return=None, error_message="Operation failed"):
    try:
        return action()

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return default_return

    except ClientError as e:
        logging.error(f"{error_message}: {e}")
        return default_return

    except Exception as e:
        logging.error(f"{error_message}: {e}")
        return default_return


# MF01.v1 - MonsterForge Cleaning Pipeline
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
    df = trim_string_columns(df)

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
    ]

    return select_columns(df, expected_columns)


# MF02.v1 - Split Clean And Quarantine
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


# MF03.v1 - Load MonsterForge Raw Data
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


# MF04.v1 - Generate MonsterForge Quality Report
def generate_quality_report(df_cleaned, clean_df, quarantine_df):
    """Generate MonsterForge data quality metrics."""
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
    }


# MF05.v1 - Upload Pandas CSV To S3
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


# MF06.v1 - Write Clean Data To S3
def write_clean_data_to_s3(clean_df, bucket_name: str):
    """Write clean MonsterForge records to S3 as CSV."""
    log_step("Writing Clean Data To S3")

    pandas_df = clean_df.toPandas()

    return upload_pandas_csv_to_s3(
        pandas_df=pandas_df,
        bucket_name=bucket_name,
        s3_key="clean/monsters/monsters_clean.csv",
    )


# MF07.v1 - Write Quarantine Data To S3
def write_quarantine_data_to_s3(quarantine_df, bucket_name: str):
    """Write quarantined MonsterForge records to S3 as CSV."""
    log_step("Writing Quarantine Data To S3")

    pandas_df = quarantine_df.toPandas()

    return upload_pandas_csv_to_s3(
        pandas_df=pandas_df,
        bucket_name=bucket_name,
        s3_key="quarantine/monsters/monsters_quarantine.csv",
    )


# MF08.v1 - Write Quality Report To S3
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


# MF09.v1 - Run MonsterForge ETL
def run_monsterforge_etl(
    spark: SparkSession,
    input_path: str,
    bucket_name: str = None,
    preview: bool = True,
):
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


if __name__ == "__main__":

    response = create_glue_crawler(
        crawler_name="monsterforge-quarantine-crawler",
        database_name="monsterforge_data_lake",
        role_arn="arn:aws:iam::873851887650:role/AWSGlueServiceRole-OlistCrawler",
        s3_target_path="s3://wlmdatawizard-monsterforge-873851887650/quarantine/monsters/",
        description="MonsterForge quarantine crawler",
    )

    if response:
        print("Crawler created successfully.")
    else:
        print("Crawler creation failed.")

    # spark = (
    #     SparkSession.builder
    #     .appName("MonsterForge ETL")
    #     .getOrCreate()
    # )

    # spark.sparkContext.setLogLevel("ERROR")

    # input_path = "data/monster/MonsterForge_monsters_raw_100.csv"
    # bucket_name = "wlmdatawizard-monsterforge-873851887650"

    # clean_df, quarantine_df, report = run_monsterforge_etl(
    #     spark=spark,
    #     input_path=input_path,
    #     bucket_name=bucket_name,
    #     preview=True,
    # )

    # log_step("MonsterForge ETL Complete")
