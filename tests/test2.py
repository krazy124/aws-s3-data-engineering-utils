from pathlib import Path
from pyspark.sql.functions import (col, lit, lower, upper, trim, regexp_replace, when, coalesce,
                                   to_date, date_format, current_timestamp, concat, concat_ws, split,
                                   substring, count, avg,  expr, countDistinct, initcap)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, expr
import boto3
import logging
from botocore.exceptions import ClientError, NoCredentialsError
from pprint import pprint
from datetime import datetime, time
import pandas as pd
import time
import os

s3 = boto3.client("s3")
glue = boto3.client('glue')
athena = boto3.client('athena')

bucket_name = "wlmdatawizard-monsterforge-873851887650"
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
latest_key = "raw/monsters/latest/monsters.csv"
run_key = f"raw/monsters/runs/run_id={run_id}/monsters.csv"
file = r"C:\Users\willi\OneDrive\Desktop\aws_project\aws-s3-data-engineering-utils\data\monster\MonsterForge_monsters_raw_100.csv"


def run_safely(func, *args, default_return=None, error_message="Operation failed", **kwargs):
    try:
        return func(*args, **kwargs)

    except NoCredentialsError:
        logging.error(
            "AWS credentials not configured."
        )
        return default_return

    except ClientError as e:
        logging.error(
            f"{error_message} | "
            f"AWS Error: {e.response['Error']['Code']} | "
            f"{e.response['Error']['Message']}"
        )
        return default_return

    except Exception as e:
        logging.error(
            f"{error_message} | "
            f"{type(e).__name__}: {e}"
        )
        return default_return


results_raw_latest = run_safely(s3.upload_file, Filename=file, Bucket=bucket_name, Key=latest_key, error_message="Failed to upload latest raw file")
results_raw_run = run_safely(s3.upload_file, Filename=file, Bucket=bucket_name, Key=run_key, error_message="Failed to upload run raw file")


spark = (SparkSession.builder .appName("MonsterForge") .getOrCreate())

df = spark.read.csv(file, header=True, inferSchema=True)


def damage_report(df):

    row_count = df.count()

    print("\n========== FINDINGS ==========")

    # Duplicate monster IDs
    if "Monster ID" in df.columns:
        id_column = "Monster ID"
    elif "monster_id" in df.columns:
        id_column = "monster_id"
    else:
        id_column = None

    if id_column:
        distinct_id_count = df.select(id_column).distinct().count()
        duplicate_id_count = row_count - distinct_id_count

        if duplicate_id_count > 0:
            print(f"WARNING: {duplicate_id_count} duplicate {id_column} values detected")
        else:
            print(f"OK: No duplicate {id_column} values detected")

    # Nulls by column
    for column_name in df.columns:
        null_count = df.filter(col(column_name).isNull()).count()

        if null_count > 0:
            print(f"WARNING: {null_count} null values detected in {column_name}")

    # Distinct info for watched columns
    watch_columns = [
        "Monster Type", "monster_type",
        "Status", "status",
        "Danger Level", "danger_level"
    ]

    for column_name in watch_columns:
        if column_name in df.columns:
            distinct_count = df.select(column_name).distinct().count()
            print(f"INFO: {column_name} has {distinct_count} distinct values")

    print("\n========== COLUMN PROFILE ==========")

    rows = []

    for column_name, data_type in df.dtypes:
        row = {"column": column_name, "schema": data_type, "null_count": str(df.filter(col(column_name).isNull()).count()),
               "blank_count": (str(df.filter(trim(col(column_name)) == "").count()) if data_type == "string" else "N/A"),
               "distinct_count": str(df.select(column_name).distinct().count())}

        rows.append(row)

    columns = ["column", "schema", "null_count", "blank_count", "distinct_count"]

    # calculate column widths
    widths = {}
    for column in columns:
        widths[column] = max(len(column), max(len(str(row.get(column, ""))) for row in rows))

    # print header
    header = " | ".join(column.ljust(widths[column]) for column in columns)
    divider = "-+-".join("-" * widths[column] for column in columns)

    print(header)
    print(divider)

    # print rows
    for row in rows:
        line = " | ".join(str(row.get(column, "")).ljust(widths[column]) for column in columns)
        print(line)
    print("\n")


pipeline_plan = [
    ("normalize_headers", "all_columns"),
    ("trim", "all_string_columns"),
    ("blank_to_null", "all_string_columns"),

    ("upper", ["monster_id"]),
    ("initcap", ["monster_name"]),
    ("lower", ["monster_type", "status"]),

    ("preserve_original", ["created_date", "danger_level", "base_price"]),

    ("clean_currency", ["base_price"]),
    ("try_cast_double", ["base_price"]),

    ("flag_negative", ["base_price"]),
    ("fix_negative", ["base_price"]),

    ("try_parse_date", ["created_date"]),
    ("try_cast_int", ["danger_level"]),

    ("range_check", ["danger_level"]),
    ("null_check", ["monster_name", "created_date"]),
]


def before_transformations(df):
    print("\n-------------------------------Before Transformations--------------------------------")
    damage_report(df)
    print("============================= Before Transformations =============================")
    df.show(30)
    print("\n========== Required Transformation Order ============================")
    for step_num, (transform_name, columns) in enumerate(pipeline_plan, start=1):
        print(f"{step_num:02d}. {transform_name:<20} {columns}")
    print("=======================================================================")


def after_transformations(df, clean_df, quarantine_df):
    print("\n========== After Transformations ==========")
    clean_df.show(30)
    print("\n========== CLEAN / QUARANTINE SUMMARY ==================")
    print(f"Total Rows: {df.count()}")
    print(f"Clean Rows: {clean_df.count()}")
    print(f"Quarantine Rows: {quarantine_df.count()}")
    damage_report(clean_df)


def transform_data(df):
    before_transformations(df)

    for old_name in df.columns:
        new_name = old_name.lower().replace(" ", "_")
        df = df.withColumnRenamed(old_name, new_name)

    for column_name, data_type in df.dtypes:
        if data_type == "string":
            df = df.withColumn(column_name, trim(col(column_name)))

    for column_name, data_type in df.dtypes:
        if data_type == "string":
            df = df.withColumn(column_name, when(trim(col(column_name)) == "", None) .otherwise(col(column_name)))

    df = df.withColumn("monster_id", upper(col("monster_id")))
    df = df.withColumn("monster_name", initcap(col("monster_name")))
    df = df.withColumn("monster_type", lower(col("monster_type")))
    df = df.withColumn("status", lower(col("status")))
    df = df.withColumn("preserve_date", col("created_date"))
    df = df.withColumn("preserve_danger", col("danger_level"))
    df = df.withColumn("preserve_price", col("base_price"))
    df = df.withColumn("base_price", regexp_replace("base_price", r"[$,]", ""))
    df = df.withColumn("base_price", regexp_replace("base_price", r"USD\s", ""))
    df = df.withColumn("base_price", col("base_price").cast("double"))
    df = df.withColumn("original_price_negative", when(col("base_price") < 0, True).otherwise(False))
    df = df.withColumn("base_price", when(col("base_price") < 0, col("base_price") * -1) .otherwise(col("base_price")))
    df = df.withColumn("created_date", coalesce(expr("try_to_date(`created_date`, 'M/d/yy')"), expr("try_to_date(`created_date`, 'yyyy-MM-dd')"),
                                                expr("try_to_date(`created_date`, 'yyyy/MM/dd')"), expr("try_to_date(`created_date`, 'MMM d yyyy')"), expr("try_to_date(`created_date`, 'MM-dd-yyyy')")))
    df = df.withColumn("danger_level", expr("try_cast(danger_level as int)"))
    df = df.withColumn("danger_level", when((col("danger_level") < 1) | (col("danger_level") > 10), None).otherwise(col("danger_level")))
    duplicate_rows = (df.groupBy(df.columns) .count() .filter(col("count") > 1) .drop("count"))
    duplicate_rows = duplicate_rows.withColumn("duplicate_row", lit(True))
    df = (df.join(duplicate_rows, on=df.columns, how="left") .fillna(False, subset=["duplicate_row"]))

    quarantine_condition = (col("monster_name").isNull() | col("created_date").isNull() | col("danger_level").isNull() | col("duplicate_row"))

    quarantine_df = df.filter(quarantine_condition)

    clean_condition = (col("monster_name").isNotNull() & col("created_date").isNotNull() & col("danger_level").isNotNull() & (col("duplicate_row") == False))

    clean_df = df.filter(clean_condition)

    after_transformations(df, clean_df, quarantine_df)
    return clean_df, quarantine_df


def export_to_local(clean_df, quarantine_df, run_id):
    output_root = Path.cwd() / "output" / f"run_id={run_id}"
    output_root.mkdir(parents=True, exist_ok=True)

    clean_local_file = output_root / "monsters_clean.csv"
    quarantine_local_file = output_root / "monsters_quarantine.csv"

    print(f"\nExporting files to: {output_root}")

    clean_df.toPandas().to_csv(clean_local_file, index=False)
    quarantine_df.toPandas().to_csv(quarantine_local_file, index=False)

    print("CSV export complete.")
    print(f"✓ {clean_local_file.name}")
    print(f"✓ {quarantine_local_file.name}")
    return clean_local_file, quarantine_local_file


def export_to_s3(clean_local_file, quarantine_local_file, run_id):
    clean_latest_key = "clean/monsters/latest/monsters_clean.csv"
    clean_run_key = f"clean/monsters/runs/run_id={run_id}/monsters_clean.csv"
    clean_table_key = "clean/monsters/table/monsters_clean.csv"

    results_clean_latest = run_safely(s3.upload_file, Filename=str(clean_local_file), Bucket=bucket_name, Key=clean_latest_key, error_message="Failed to upload latest clean file")
    results_clean_run = run_safely(s3.upload_file, Filename=str(clean_local_file), Bucket=bucket_name, Key=clean_run_key, error_message="Failed to upload run clean file")
    results_clean_table = run_safely(s3.upload_file, Filename=str(clean_local_file), Bucket=bucket_name, Key=clean_table_key, error_message="Failed to upload clean table file")

    quarantine_latest_key = "quarantine/monsters/latest/monsters_quarantine.csv"
    quarantine_run_key = f"quarantine/monsters/runs/run_id={run_id}/monsters_quarantine.csv"
    quarantine_table_key = "quarantine/monsters/table/monsters_quarantine.csv"

    results_quarantine_latest = run_safely(s3.upload_file, Filename=str(quarantine_local_file), Bucket=bucket_name, Key=quarantine_latest_key, error_message="Failed to upload latest quarantine file")
    results_quarantine_run = run_safely(s3.upload_file, Filename=str(quarantine_local_file), Bucket=bucket_name, Key=quarantine_run_key, error_message="Failed to upload run quarantine file")
    results_quarantine_table = run_safely(s3.upload_file, Filename=str(quarantine_local_file), Bucket=bucket_name, Key=quarantine_table_key, error_message="Failed to upload quarantine table file")

    all_keys = [clean_latest_key, clean_run_key, clean_table_key, quarantine_latest_key, quarantine_run_key, quarantine_table_key, ]

    print("\n========== S3 UPLOAD VERIFICATION ==========")

    for key in all_keys:
        response = run_safely(s3.head_object, Bucket=bucket_name, Key=key, error_message=f"Failed to verify {key}")
        if response:
            print(f"✓ Verified: s3://{bucket_name}/{key} | {response['ContentLength']} bytes")


def run_glue_crawlers():
    print("\n========== GLUE CRAWLERS ==========")

    crawlers = ["monsterforge-clean-crawler", "monsterforge-quarantine-crawler"]

    for crawler_name in crawlers:
        run_safely(glue.start_crawler, Name=crawler_name, error_message=f"Failed to start {crawler_name}")
        previous_state = None
        while True:
            response = run_safely(glue.get_crawler, Name=crawler_name, error_message=f"Failed to check {crawler_name} status")
            if response is None:
                break
            state = response["Crawler"]["State"]
            if state != previous_state:
                print(f"{crawler_name}: {state}")
                previous_state = state
            if state == "READY":
                break
            time.sleep(10)

    print("✓ Glue crawlers completed")


clean_df, quarantine_df = transform_data(df)
clean_local_file, quarantine_local_file = export_to_local(clean_df, quarantine_df, run_id)
export_to_s3(clean_local_file, quarantine_local_file, run_id)
# run_glue_crawlers()
