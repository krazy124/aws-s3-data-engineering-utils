"""Print/report/debug helpers for Glue and Athena metadata checks."""

import json
import logging
import time

import boto3

from aws_clients import run_safely
from glue_catalog_operations import (
    list_glue_databases,
    list_glue_tables,
    get_glue_table,
    get_table_schema,
    get_table_metadata_summary,
    get_table_row_count,
)
from glue_crawler_operations import (
    list_glue_crawlers,
    get_crawler_details,
    get_crawler_info,
)
from athena_operations import count_table_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

dbName = "olist_data_lake"


def show_all_crawler_details(region="us-east-1"):
    crawlers = list_glue_crawlers(region=region)

    print("\n" + "=" * 120)
    print("EXISTING GLUE CRAWLERS")
    print("=" * 120)

    if not crawlers:
        print("No crawlers found.")
        return []

    print(
        f"{'#':<4}"
        f"{'Crawler Name':<38}"
        f"{'Database':<25}"
        f"{'State':<10}"
        f"{'Target Path'}"
    )

    print("-" * 120)

    crawler_rows = []

    for index, crawler_name in enumerate(crawlers, start=1):
        crawler = get_crawler_details(
            crawler_name,
            region=region,
        )

        if not crawler:
            continue

        targets = crawler.get("targets", [])
        target_path = targets[0].get("Path", "N/A") if targets else "N/A"

        target_path = (
            target_path
            .replace("s3://wlmdatawizard-monsterforge-873851887650/", "")
            .replace("s3://wlmdatawizard-data-lake-873851887650-us-east-1-an/", "")
        )

        row = {
            "number": index,
            "name": crawler.get("name", "N/A"),
            "database": crawler.get("database", "N/A"),
            "state": crawler.get("state", "N/A"),
            "target": target_path,
        }

        crawler_rows.append(row)

        print(
            f"{row['number']:<4}"
            f"{row['name']:<38}"
            f"{row['database']:<25}"
            f"{row['state']:<10}"
            f"{row['target']}"
        )

    print("=" * 120)

    return crawler_rows


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


def test():

    response = get_glue_client("us-east-1").get_tables(DatabaseName="olist_data_lake")
    print(response.keys())
    print(response["TableList"][0]["Name"])
