import boto3
import logging
from botocore.exceptions import ClientError, NoCredentialsError
from pprint import pprint
import notes.bag_of_tricks as tricks
from datetime import datetime

import pandas as pd

from pyspark.sql import SparkSession


s3 = boto3.client("s3")
glue = boto3.client('glue')
athena = boto3.client('athena')

bucket_name = "wlmdatawizard-monsterforge-873851887650"
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
latest_key = "raw/monsters/latest/monsters.csv"
run_key = f"raw/monsters/runs/run_id={run_id}/monsters.csv"
file = r"C:\Users\willi\OneDrive\Desktop\aws project\aws-s3-data-engineering-utils\data\monster\MonsterForge_monsters_raw_100.csv"


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


# --------------------------------------------------------------

"""
Create Clients
    ↓
s3 = boto3.client("s3")
glue = boto3.client("glue")
athena = boto3.client("athena")
    ↓
Upload Raw File to S3
    ↓
Run ETL Transformations
    ↓
Write Clean Data to S3
    ↓
Write Quarantine Data to S3
    ↓
Write Quality Report to S3
    ↓
Start Glue Crawler
    ↓
Wait For Crawler To Finish
    ↓
Run Athena Query
    ↓
Wait For Query To Finish
    ↓
Retrieve Results
    ↓
Display Results
"""
