import boto3
import logging
from botocore.exceptions import ClientError, NoCredentialsError
from pprint import pprint
import notes.bag_of_tricks as tricks
from datetime import datetime

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, lit, lower, upper, trim, regexp_replace,
                                   when, coalesce, to_date, date_format, current_timestamp,
                                   concat, concat_ws, split, substring, count, sum, avg, min, max, expr,
                                   countDistinct)


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


# --------------------------------------------------------------
results1 = run_safely(s3.upload_file, Filename=file, Bucket=bucket_name, Key=latest_key, error_message="Failed to upload latest raw file")
results2 = run_safely(s3.upload_file, Filename=file, Bucket=bucket_name, Key=run_key, error_message="Failed to upload run raw file")
# --------------------------------------------------------------
spark = (SparkSession.builder .appName("MonsterForge") .getOrCreate())


df = spark.read.csv(file, header=True, inferSchema=True)

for column_name, data_type in df.dtypes:
    if data_type == "string":
        df = df.withColumn(column_name, trim(col(column_name)))

for old_name in df.columns:
    new_name = old_name.lower().replace(" ", "_")
    df = df.withColumnRenamed(old_name, new_name)

df = df.withColumn("monster_type", lower(df["monster_type"]))
df = df.withColumn("status", lower(df["status"]))
df.show(100)  # Display the DataFrame to verify the changes
df = df.withColumn("base_price", regexp_replace("base_price", r"[$,]|USD ", ""))
df = df.withColumn("base_price", col("base_price").cast("double"))
df = df.withColumn("price_was_negative", when(col("base_price") < 0, True).otherwise(False))
df = df.withColumn("base_price", regexp_replace("base_price", r"[-]", ""))
df = df.withColumn("original_date", col("created_date"))
df = df.withColumn("created_date", coalesce(expr("try_to_date(`created_date`, 'M/d/yy')"),
                                            expr("try_to_date(`created_date`, 'yyyy-MM-dd')"),
                                            expr("try_to_date(`created_date`, 'yyyy/MM/dd')"),
                                            expr("try_to_date(`created_date`, 'MMM d yyyy')"),
                                            expr("try_to_date(`created_date`, 'MM-dd-yyyy')")))
df = df.withColumn("original_danger_level", col("danger_level"))
df = df.withColumn("danger_level", expr("try_cast(danger_level as int)"))
df = df.withColumn("invalid_danger_level", col("danger_level").isNull() | (col("danger_level") < 1) | (col("danger_level") > 10))


df = df.withColumn("monster_name_is_null", col("monster_name").isNull())
df = df.withColumn("date_is_null", col("created_date").isNull())
df = df.select("monster_id", "monster_name", "monster_type", "danger_level",
               "created_date", "base_price", "status", "original_date", "original_danger_level",
               "monster_name_is_null", "date_is_null", "invalid_danger_level", "price_was_negative")

df.show(100)  # Display the DataFrame to verify the changes

# --------------------------------------------------------------
"""
All columns - trimmed -check for NULLs

monster_id - maybe check null - maybe check duplicate - maybe check pattern like M001
monster_name - check null
monster_type - lowercase - maybe validate allowed values
danger_level - keep original - try_cast int - validate 1-10
created_date - keep original - parse date - flag null
base_price - clean currency - cast double - flag negative - fix negative
status - lowercase - maybe validate allowed values"""

"""pipeline_plan = [
    ("normalize_headers", "all_columns"),
    ("trim", "all_string_columns"),
    ("blank_to_null", "all_string_columns"),
    ("copy_original", ["created_date", "danger_level"]),
    ("lower", ["monster_type", "status"]),
    ("clean_currency", ["base_price"]),
    ("try_cast_double", ["base_price"]),
    ("flag_negative", ["base_price"]),
    ("fix_negative", ["base_price"]),
    ("try_parse_date", ["created_date"]),
    ("try_cast_int", ["danger_level"]),
    ("range_check", ["danger_level"]),
    ("null_check", ["monster_name", "created_date"]),
]"""

"""pipeline_plan = [
    {
        "transformation": "trim",
        "columns": "all_string_columns"
    },
    {
        "transformation": "lower",
        "columns": ["monster_type", "status"]
    },
    {
        "transformation": "copy_original",
        "columns": ["created_date", "danger_level"]
    }
]"""

"""{
    "transformation": "try_cast_int",
    "columns": ["danger_level"],
    "new_column": "danger_level",
    "preserve_original": True
}"""

"""groupBy (to group your data by a specific column and perform aggregate functions), 
   filter (to pick rows based on conditions), select (to choose specific columns), 
   orderBy (to sort your data), join (to combine two DataFrames) df.printSchema() df.show(5)"""

""" lower(), upper(), trim(), regexp_replace(), cast(), to_date(), when(), otherwise(), isNull(), dropDuplicates(), """

""" Create Clients ↓ s3 = boto3.client("s3") glue = boto3.client("glue") athena = boto3.client("athena") 
↓ Upload Raw File to S3 ↓ Run ETL Transformations ↓ Write Clean Data to S3 ↓ Write Quarantine Data to S3 
↓ Write Quality Report to S3 ↓ Start Glue Crawler ↓ Wait For Crawler To Finish ↓ Run Athena Query 
↓ Wait For Query To Finish ↓ Retrieve Results ↓ Display Results """
