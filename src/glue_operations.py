import logging
from urllib import response

import boto3
import streamlit as st
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import pprint
import json
from sep_of_concerns import list_files_in_prefix
import time
from datetime import datetime
from athena_operations import count_table_rows, get_active_athena_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

_GLUE_CLIENT_CACHE = {}

dbName = "olist_data_lake"


def get_glue_client(region="us-east-1"):
    try:
        client = boto3.client("glue", region_name=region)

        client.get_databases()

        logging.info(
            f"Connected to AWS Glue using local/default credentials in region {region}"
        )

        return client

    except (NoCredentialsError, PartialCredentialsError) as e:
        logging.warning(f"Local/default AWS credentials not available: {e}")

    except Exception as e:
        logging.error(f"Failed local/default Glue connection attempt: {e}")

    try:
        client = boto3.client(
            "glue",
            aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
            region_name=st.secrets.get("AWS_DEFAULT_REGION", region),
        )

        client.get_databases()

        logging.info("Connected to AWS Glue using Streamlit secrets credentials")

        return client

    except Exception as e:
        logging.error(f"Failed Streamlit secrets Glue connection attempt: {e}")

    logging.error("Unable to establish AWS Glue client connection")

    return None


def get_active_glue_client(region="us-east-1", client=None):
    if client is not None:
        return client

    if region in _GLUE_CLIENT_CACHE:
        return _GLUE_CLIENT_CACHE[region]

    new_client = get_glue_client(region)

    if new_client is not None:
        _GLUE_CLIENT_CACHE[region] = new_client

    return new_client


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


def list_glue_databases(region="us-east-1", client=None):
    def action():
        glue_client = get_active_glue_client(region, client)

        response = glue_client.get_databases()

        return [
            database["Name"]
            for database in response.get("DatabaseList", [])
        ]

    return run_safely(
        action,
        default_return=[],
        error_message="Failed to list Glue databases",
    )


def list_glue_tables(database_name, region="us-east-1", client=None):
    def action():
        glue_client = get_active_glue_client(region, client)

        response = glue_client.get_tables(DatabaseName=database_name,)

        return [
            table["Name"]
            for table in response.get("TableList", [])
        ]

    return run_safely(action, default_return=[], error_message=f"Failed to list Glue tables in database {database_name}",)


def test():

    response = get_glue_client("us-east-1").get_tables(DatabaseName="olist_data_lake")
    print(response.keys())
    print(response["TableList"][0]["Name"])


def get_glue_table(database_name, table_name, region="us-east-1", client=None):
    def action():
        glue_client = get_active_glue_client(region, client)

        response = glue_client.get_table(DatabaseName=database_name, Name=table_name,)

        return response.get("Table", {})

    return run_safely(
        action,
        default_return={},
        error_message=f"Failed to get Glue table {table_name}",
    )


def get_table_schema(database_name, table_name, region="us-east-1", client=None):
    def action():
        table = get_glue_table(
            database_name,
            table_name,
            region=region,
            client=client,
        )

        storage = table.get("StorageDescriptor", {})

        return storage.get("Columns", [])

    return run_safely(
        action,
        default_return=[],
        error_message=f"Failed to get schema for table {table_name}",
    )


def get_table_metadata_summary(database_name, table_name, region="us-east-1", client=None):
    def action():
        table = get_glue_table(
            database_name,
            table_name,
            region=region,
            client=client,
        )

        storage = table.get("StorageDescriptor", {})
        serde = storage.get("SerdeInfo", {})
        parameters = table.get("Parameters", {})

        return {
            "table_name": table.get("Name"),
            "database_name": table.get("DatabaseName"),
            "location": storage.get("Location"),
            "input_format": storage.get("InputFormat"),
            "output_format": storage.get("OutputFormat"),
            "serde_library": serde.get("SerializationLibrary"),
            "classification": parameters.get("classification"),
            "columns": storage.get("Columns", []),
            "created_time": table.get("CreateTime"),
            "updated_time": table.get("UpdateTime"),
        }

    return run_safely(
        action,
        default_return={},
        error_message=f"Failed to summarize metadata for table {table_name}",
    )


def list_glue_crawlers(region="us-east-1", client=None):
    def action():
        glue_client = get_active_glue_client(region, client)

        response = glue_client.list_crawlers()

        return response.get("CrawlerNames", [])

    return run_safely(
        action,
        default_return=[],
        error_message="Failed to list Glue crawlers",
    )


def create_glue_crawler(crawler_name: str, database_name: str, role_arn: str, s3_target_path: str, description: str = "", region: str = "us-east-1", client=None, ):
    def action():
        glue_client = get_active_glue_client(region, client)

        if glue_client is None:
            logging.error("No active Glue client available.")
            return None

        response = glue_client.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=database_name,
            Description=description,
            Targets={
                "S3Targets": [
                    {
                        "Path": s3_target_path
                    }
                ]
            },
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",
            },
            RecrawlPolicy={
                "RecrawlBehavior": "CRAWL_EVERYTHING",
            },
        )

        logging.info(f"Created crawler: {crawler_name}")
        return response

    return run_safely(
        action,
        default_return=None,
        error_message=f"Failed to create crawler: {crawler_name}",
    )


def start_crawler(crawler_name, region="us-east-1", client=None):
    def action():
        glue_client = get_active_glue_client(region, client)

        glue_client.start_crawler(
            Name=crawler_name,
        )

        logging.info(f"Started Glue crawler: {crawler_name}")

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to start Glue crawler {crawler_name}",
    )


def get_crawler_status(crawler_name, region="us-east-1", client=None):
    def action():
        crawler = get_crawler_info(
            crawler_name,
            region=region,
            client=client,
        )

        return crawler.get("State")

    return run_safely(
        action,
        default_return=None,
        error_message=f"Failed to get crawler status for {crawler_name}",
    )


def run_crawler_workflow(crawler_name, region="us-east-1", client=None):
    print("\n" + "=" * 70)
    print(f"CRAWLER WORKFLOW: {crawler_name}")
    print("=" * 70)

    print("\nChecking crawler info...")
    crawler_info = get_crawler_info(crawler_name, region, client)

    if not crawler_info:
        print("Crawler not found or could not be accessed.")
        return False

    current_status = get_crawler_status(crawler_name, region, client)
    print(f"Current Status: {current_status}")

    if current_status == "RUNNING":
        print("Crawler is already running. Waiting for it to finish...")
    else:
        print("\nStarting crawler...")
        started = start_crawler(crawler_name, region, client)

        if not started:
            print("Crawler failed to start.")
            return False

    print("\nWaiting for crawler to finish...")

    max_attempts = 60
    delay_seconds = 5

    for attempt in range(max_attempts):
        status = get_crawler_status(crawler_name, region, client)

        print(f"Attempt {attempt + 1}: {crawler_name} status = {status}")

        if status == "READY":
            print(f"\nCrawler finished: {crawler_name}")
            return True

        time.sleep(delay_seconds)

    print(f"\nCrawler timed out: {crawler_name}")
    return False


def list_glue_jobs(region="us-east-1", client=None):
    def action():
        glue_client = get_active_glue_client(region, client)

        response = glue_client.list_jobs()

        return response.get("JobNames", [])

    return run_safely(
        action,
        default_return=[],
        error_message="Failed to list Glue jobs",
    )


def get_glue_job_info(job_name, region="us-east-1", client=None):
    def action():
        glue_client = get_active_glue_client(region, client)

        response = glue_client.get_job(
            JobName=job_name,
        )

        return response.get("Job", {})

    return run_safely(
        action,
        default_return={},
        error_message=f"Failed to get Glue job info for {job_name}",
    )


def start_glue_job(job_name, arguments=None, region="us-east-1", client=None):
    if arguments is None:
        arguments = {}

    def action():
        glue_client = get_active_glue_client(region, client)

        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=arguments,
        )

        job_run_id = response.get("JobRunId")

        logging.info(f"Started Glue job {job_name} with run ID {job_run_id}")

        return job_run_id

    return run_safely(
        action,
        default_return=None,
        error_message=f"Failed to start Glue job {job_name}",
    )


def get_glue_job_run(job_name, job_run_id, region="us-east-1", client=None):
    def action():
        glue_client = get_active_glue_client(region, client)

        response = glue_client.get_job_run(
            JobName=job_name,
            RunId=job_run_id,
        )

        return response.get("JobRun", {})

    return run_safely(
        action,
        default_return={},
        error_message=f"Failed to get Glue job run {job_run_id} for job {job_name}",
    )


def get_glue_job_status(job_name, job_run_id, region="us-east-1", client=None):
    def action():
        job_run = get_glue_job_run(
            job_name,
            job_run_id,
            region=region,
            client=client,
        )

        return job_run.get("JobRunState")

    return run_safely(
        action,
        default_return=None,
        error_message=f"Failed to get Glue job status for {job_name}",
    )


def stop_glue_job_run(job_name, job_run_id, region="us-east-1", client=None):
    def action():
        glue_client = get_active_glue_client(region, client)

        glue_client.batch_stop_job_run(
            JobName=job_name,
            JobRunIds=[job_run_id],
        )

        logging.info(f"Stopped Glue job {job_name} run {job_run_id}")

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to stop Glue job {job_name} run {job_run_id}",
    )


def get_full_table_info(database_name, table_name, region="us-east-1", client=None):
    table = get_glue_table(
        database_name,
        table_name,
        region=region,
        client=client,
    )

    return table


def print_glue_databases(region="us-east-1", client=None):
    databases = list_glue_databases(region, client)

    print("\nGlue Databases")
    print("-" * 40)

    if not databases:
        print("No Glue databases found.")
        return

    for index, database in enumerate(databases, start=1):
        print(f"{index}. {database}")


def print_glue_tables(database_name, region="us-east-1", client=None):
    tables = list_glue_tables(database_name, region, client)

    print(f"\nGlue Tables in Database: {database_name}")
    print("-" * 40)

    if not tables:
        print("No Glue tables found.")
        return

    for index, table in enumerate(tables, start=1):
        print(f"{index}. {table}")


def print_table_schema(database_name, table_name, region="us-east-1", client=None):
    schema = get_table_schema(database_name, table_name, region, client)

    print(f"\nSchema for Table: {table_name}")
    print("-" * 60)

    if not schema:
        print("No schema found.")
        return

    for column in schema:
        name = column.get("Name", "Unknown")
        data_type = column.get("Type", "Unknown")
        print(f"{name:<35} {data_type}")


def print_table_metadata_summary(database_name, table_name, region="us-east-1", client=None):
    metadata = get_table_metadata_summary(database_name, table_name, region, client)

    print(f"\nMetadata Summary for Table: {table_name}")
    print("-" * 60)

    if not metadata:
        print("No metadata found.")
        return

    print(f"Table Name:      {metadata.get('table_name')}")
    print(f"Database Name:   {metadata.get('database_name')}")
    print(f"Location:        {metadata.get('location')}")
    print(f"Classification:  {metadata.get('classification')}")
    print(f"Serde Library:   {metadata.get('serde_library')}")
    print(f"Input Format:    {metadata.get('input_format')}")
    print(f"Output Format:   {metadata.get('output_format')}")
    print(f"Created Time:    {metadata.get('created_time')}")
    print(f"Updated Time:    {metadata.get('updated_time')}")


def print_table_details(database_name, table_name, region="us-east-1", client=None):
    table = get_glue_table(database_name, table_name, region, client)
    storage = table.get("StorageDescriptor", {})
    serde = storage.get("SerdeInfo", {})
    columns = storage.get("Columns", [])
    partition_keys = table.get("PartitionKeys", [])
    parameters = table.get("Parameters", {})

    print("\n" + "=" * 70)
    print("TABLE INFORMATION")
    print("=" * 70)
    print(f"Name:          {table.get('Name')}")
    print(f"Database:      {table.get('DatabaseName')}")
    print(f"Table Type:    {table.get('TableType')}")
    print(f"Owner:         {table.get('Owner')}")
    print(f"Created:       {table.get('CreateTime')}")
    print(f"Updated:       {table.get('UpdateTime')}")

    print("\n" + "=" * 70)
    print("STORAGE INFORMATION")
    print("=" * 70)
    print(f"Location:      {storage.get('Location')}")
    print(f"Input Format:  {storage.get('InputFormat')}")
    print(f"Output Format: {storage.get('OutputFormat')}")
    print(f"Serde Name:    {serde.get('Name')}")
    print(f"Serde Library: {serde.get('SerializationLibrary')}")

    print("\n" + "=" * 70)
    print("COLUMNS")
    print("=" * 70)

    if columns:
        for column in columns:
            print(f"{column.get('Name'):<35} {column.get('Type')}")
    else:
        print("No columns found.")

    print("\n" + "=" * 70)
    print("PARTITION KEYS")
    print("=" * 70)

    if partition_keys:
        for partition in partition_keys:
            print(f"{partition.get('Name'):<35} {partition.get('Type')}")
    else:
        print("No partition keys defined.")

    print("\n" + "=" * 70)
    print("PARAMETERS")
    print("=" * 70)

    if parameters:
        for key, value in parameters.items():
            print(f"{key:<35} {value}")
    else:
        print("No parameters found.")

    print("\n" + "=" * 70)
    print("FULL RAW TABLE OBJECT")
    print("=" * 70)
    print(json.dumps(table, indent=4, default=str))


def print_table_summary_report(database_name, table_name, region="us-east-1", client=None):
    table_metadata = get_glue_table(
        database_name,
        table_name,
        region=region,
        client=client,
    )

    parameters = table_metadata.get("Parameters", {})
    storage = table_metadata.get("StorageDescriptor", {})

    print("\n" + "=" * 70)
    print(f"TABLE SUMMARY: {table_name}")
    print("=" * 70)

    print(f"Table Name:           {table_metadata.get('Name')}")
    print(f"Record Count:         {parameters.get('recordCount', 'N/A')}")
    print(f"Size:                 {parameters.get('sizeKey', 'N/A')}")
    print(f"Average Record Size:  {parameters.get('averageRecordSize', 'N/A')}")
    print(f"Classification:       {parameters.get('classification', 'N/A')}")
    print(f"Location:             {storage.get('Location', 'N/A')}")
    print(f"Updated Time:         {table_metadata.get('UpdateTime', 'N/A')}")


def inspect_csv_header_from_s3(bucket_name, object_key, region="us-east-1", client=None):
    def action():
        s3_client = (
            client
            if client is not None
            else boto3.client("s3", region_name=region)
        )

        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=object_key,
            Range="bytes=0-2000",
        )

        content = response["Body"].read().decode(
            "utf-8",
            errors="replace",
        )

        print("\n" + "=" * 70)
        print(f"CSV PREVIEW: {object_key}")
        print("=" * 70)
        print(content)

        return content

    return run_safely(
        action,
        default_return="",
        error_message=f"Failed to inspect CSV file {object_key}",
    )


def print_crawler_log_events(crawler_name, region="us-east-1", client=None):

    def action():
        logs_client = (client if client is not None else boto3.client("logs", region_name=region))

        response = logs_client.get_log_events(logGroupName="/aws-glue/crawlers", logStreamName=crawler_name, limit=50, startFromHead=False, )

        print("\n" + "=" * 70)
        print(f"LOG EVENTS FOR: {crawler_name}")
        print("=" * 70)

        for event in response.get("events", []):
            print(event.get("message"))

        return response.get("events", [])

    return run_safely(action, default_return=[], error_message=f"Failed to print crawler log events for {crawler_name}", )


def print_flat_metadata(data, prefix=""):
    if isinstance(data, dict):
        for key, value in data.items():
            new_prefix = f"{prefix}.{key}" if prefix else key
            print_flat_metadata(value, new_prefix)

    elif isinstance(data, list):
        for index, item in enumerate(data):
            new_prefix = f"{prefix}[{index}]"
            print_flat_metadata(item, new_prefix)

    else:
        print(f"{prefix} = {data}")


def wait_for_crawler(crawler_name, region="us-east-1", client=None, delay=5, max_attempts=60, ):
    for attempt in range(max_attempts):
        status = get_crawler_status(crawler_name, region=region, client=client, )

        print(
            f"Attempt {attempt + 1}: "
            f"{crawler_name} = {status}"
        )

        if status == "READY":
            print(f"{crawler_name} finished.")
            return True

        time.sleep(delay)

    print(f"{crawler_name} timed out.")
    return False


def get_table_row_count(database_name, table_name, region="us-east-1", client=None, ):
    def action():
        glue_client = get_active_glue_client(region, client, )

        response = glue_client.get_table(DatabaseName=database_name, Name=table_name, )

        return int(response["Table"] .get("Parameters", {}) .get("recordCount", 0))

    return run_safely(action, default_return=0, error_message=f"Failed to get row count for {table_name}", )


def mini_glue_dahsboard():
    tables_list = list_glue_tables(dbName)
    crawlers_list = list_glue_crawlers()
    total_tables = len(tables_list)
    total_crawlers = len(crawlers_list)
    ready_crawlers = 0
    total_rows = 0
    total_size = 0

    print(f"Database: {dbName} \n\nTables:")

    for index, table in enumerate(tables_list):
        tables_meta = get_glue_table(dbName, table)
        print(
            f"{index + 1}. {table:<10} | "
            f"rows: {tables_meta['Parameters'].get('recordCount', 'N/A'):>6} | "
            f"size: {tables_meta['Parameters'].get('sizeKey', 'N/A'):>8} | "
            f"columns: {len(tables_meta['StorageDescriptor']['Columns'])}"
        )
        total_rows += int(tables_meta['Parameters'].get('recordCount', 0))
        total_size += int(tables_meta['Parameters'].get('sizeKey', 0))

    print(f"\nCrawlers:")

    for index, crawler in enumerate(crawlers_list):
        crawler_info = get_crawler_info(crawler)
        last_crawl = crawler_info.get('LastCrawl', {})

        print(
            f"{index + 1}. {crawler:<24} | "
            f"status: {crawler_info.get('State', 'N/A')} | "
            f" {last_crawl.get('Status', 'N/A')} |"
            f" {last_crawl.get('StartTime', 'N/A')}"
        )
        if crawler_info.get('State') == 'READY':
            ready_crawlers += 1

    print(
        f"\n\nSUMMARY\n"
        f"{'-' * 40}\n"
        f"{'Total Tables:':<20} {total_tables}\n"
        f"{'Total Rows:':<20} {total_rows:,}\n"
        f"{'Total Size:':<20} {total_size:,} bytes\n"
        f"{'Total Crawlers:':<20} {total_crawlers}\n"
        f"{'Crawlers Ready:':<20} {ready_crawlers}\n"
    )


def compare_table_counts(glue_client, athena_client, table_name, database, output_location):
    glue_rows = get_table_row_count(database, table_name, client=glue_client, )
    athena_result = count_table_rows(table_name, database, output_location, client=athena_client, )

    athena_rows = int(athena_result[0]["total_rows"]) if athena_result else 0
    difference = athena_rows - glue_rows

    print("\n" + "=" * 70)
    print(f"{'Table:':>17} {table_name}")
    print(f"{'Glue Row Count:':>17} {glue_rows}")
    print(f"{'Athena Row Count:':>17} {athena_rows}")
    print(f"{'Difference:':>17} {difference}")
    print("=" * 70)

    return {"table": table_name, "glue_rows": glue_rows, "athena_rows": athena_rows, "difference": difference, }


if __name__ == "__main__":
    database_name = "olist_data_lake"
    table_name = "customers"
    output_location = "s3://wlmdatawizard-data-lake-873851887650-us-east-1-an/athena-results/"
    glue_client = get_active_glue_client()
    athena_client = get_active_athena_client()

    comparison = compare_table_counts(glue_client, athena_client, table_name, database_name, output_location)
