# monsterforge_etl.py
# # MonsterForge Industries ETL Pipeline
# # Project-specific pipeline built on reusable PySpark transformation helpers

import logging
from io import StringIO
import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when
from glue_transformations import (log_step, print_quality_report, clean_column_names,
                                  trim_string_columns, add_missing_flag,
                                  standardize_category_column, parse_currency_to_double,
                                  fix_negative_values_with_flag, parse_multiple_date_formats,
                                  convert_to_integer, select_columns, )


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

_ATHENA_CLIENT_CACHE = {}


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


def get_athena_client(region="us-east-1"):
    def local_connection():
        client = boto3.client("athena", region_name=region)
        client.list_work_groups()
        logging.info(f"Connected to Athena using local/default credentials in {region}")
        return client

    client = run_safely(
        local_connection,
        default_return=None,
        error_message="Local Athena connection failed",
    )

    if not client:
        def streamlit_connection():
            client = boto3.client(
                "athena",
                aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
                region_name=st.secrets.get("AWS_DEFAULT_REGION", region),
            )

            client.list_work_groups()
            logging.info("Connected to Athena using Streamlit secrets")
            return client

        client = run_safely(
            streamlit_connection,
            default_return=None,
            error_message="Streamlit Athena connection failed",
        )

    return client


def get_active_athena_client(region="us-east-1", client=None):
    if client is not None:
        return client

    if region in _ATHENA_CLIENT_CACHE:
        logging.info(f"Using cached Athena client for {region}")
        return _ATHENA_CLIENT_CACHE[region]

    new_client = get_athena_client(region)

    if new_client is not None:
        _ATHENA_CLIENT_CACHE[region] = new_client

    return new_client


def start_query(query, database, output_location, workgroup="primary", region="us-east-1", client=None):
    def action():
        athena_client = get_active_athena_client(region, client)

        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location},
            WorkGroup=workgroup,
        )

        query_execution_id = response["QueryExecutionId"]
        logging.info(f"Started Athena query: {query_execution_id}")

        return query_execution_id

    return run_safely(
        action,
        default_return=None,
        error_message="Failed to start Athena query",
    )


def get_query_status(query_execution_id, region="us-east-1", client=None):
    def action():
        athena_client = get_active_athena_client(region, client)

        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        status = response["QueryExecution"]["Status"]["State"]

        return status, response

    return run_safely(
        action,
        default_return=("FAILED", None),
        error_message="Failed to get Athena query status",
    )


def wait_for_query(query_execution_id, poll_seconds=2, timeout_seconds=60, region="us-east-1", client=None):
    start_time = time.time()

    while True:
        status, response = get_query_status(
            query_execution_id,
            region=region,
            client=client,
        )

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            return status, response

        if time.time() - start_time > timeout_seconds:
            logging.error("Athena query timed out.")
            return "TIMEOUT", response

        time.sleep(poll_seconds)


def get_query_results(query_execution_id, region="us-east-1", client=None):
    def action():
        athena_client = get_active_athena_client(region, client)

        paginator = athena_client.get_paginator("get_query_results")
        pages = paginator.paginate(QueryExecutionId=query_execution_id)

        columns = []
        rows = []

        for page in pages:
            result_rows = page["ResultSet"]["Rows"]

            for row in result_rows:
                values = [
                    clean_athena_value(
                        column.get("VarCharValue", None)
                    )
                    for column in row.get("Data", [])
                ]

                if not columns:
                    columns = values
                    continue

                rows.append(dict(zip(columns, values)))

        return rows

    return run_safely(
        action,
        default_return=[],
        error_message="Failed to get Athena query results",
    )


def run_query(query, database, output_location, workgroup="primary", region="us-east-1", client=None):
    query_execution_id = start_query(
        query=query,
        database=database,
        output_location=output_location,
        workgroup=workgroup,
        region=region,
        client=client,
    )

    if not query_execution_id:
        return []

    status, response = wait_for_query(
        query_execution_id=query_execution_id,
        region=region,
        client=client,
    )

    if status != "SUCCEEDED":
        reason = "No reason provided"

        if response:
            reason = response["QueryExecution"]["Status"].get(
                "StateChangeReason",
                "No reason provided",
            )

        logging.error(f"Athena query failed. Status: {status}. Reason: {reason}")
        return []

    return get_query_results(
        query_execution_id=query_execution_id,
        region=region,
        client=client,
    )


def show_tables(database, output_location, workgroup="primary", region="us-east-1", client=None):
    query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{database}' ORDER BY table_name"
    results = run_query(query=query, database=database, output_location=output_location, workgroup=workgroup, region=region, client=client)
    return flatten_single_column_results(results)


def count_table_rows(table_name, database, output_location, workgroup="primary", region="us-east-1", client=None):
    query = f"""
    SELECT COUNT(*) AS total_rows
    FROM {table_name}
    """

    return run_query(
        query=query,
        database=database,
        output_location=output_location,
        workgroup=workgroup,
        region=region, client=client,
    )


def preview_table(table_name, database, output_location, limit=10, workgroup="primary", region="us-east-1", client=None):
    query = f"""
    SELECT *
    FROM {table_name}
    LIMIT {limit}
    """

    return run_query(
        query=query,
        database=database,
        output_location=output_location,
        workgroup=workgroup,
        region=region,
        client=client,
    )


def describe_table(table_name, database, output_location, workgroup="primary", region="us-east-1", client=None):
    query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '{database}' AND table_name = '{table_name}' ORDER BY ordinal_position"

    return run_query(query=query, database=database, output_location=output_location, workgroup=workgroup, region=region, client=client)


def mini_athena_dashboard(database, output_location, region="us-east-1", workgroup="primary", client=None):
    tables = show_tables(
        database=database,
        output_location=output_location,
        region=region,
        workgroup=workgroup,
        client=client,
    )

    print("\nAthena Tables")
    print("-" * 40)

    if not tables:
        print("No tables found or query failed.")
        return

    for table in tables:
        print(table)


def flatten_single_column_results(results):
    return [list(row.values())[0] for row in results if row]


def clean_athena_value(value):
    if isinstance(value, str):
        return value.strip('"')
    return value


def query_to_dataframe(query, database, output_location, workgroup="primary", region="us-east-1", client=None):
    results = run_query(query=query, database=database, output_location=output_location,
                        workgroup=workgroup, region=region, client=client, )

    return pd.DataFrame(results)


if __name__ == "__main__":
    database = "olist_data_lake"
    output_location = "s3://wlmdatawizard-data-lake-873851887650-us-east-1-an/athena-results/"

    athena_client = get_active_athena_client()

    query = """
    SELECT customer_state,
           COUNT(*) AS total_customers
    FROM customers
    GROUP BY customer_state
    ORDER BY total_customers DESC
    LIMIT 10
    """

    print("\nDATAFRAME TEST")
    print("-" * 40)

    df = query_to_dataframe(
        query,
        database,
        output_location,
        client=athena_client,
    )

    print(df)
