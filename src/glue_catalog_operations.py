"""AWS Glue Data Catalog database, table, schema, and metadata helpers."""

import logging

from aws_clients import get_active_glue_client, run_safely

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


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


def get_full_table_info(database_name, table_name, region="us-east-1", client=None):
    table = get_glue_table(
        database_name,
        table_name,
        region=region,
        client=client,
    )

    return table


def get_table_row_count(database_name, table_name, region="us-east-1", client=None, ):
    def action():
        glue_client = get_active_glue_client(region, client, )

        response = glue_client.get_table(DatabaseName=database_name, Name=table_name, )

        return int(response["Table"] .get("Parameters", {}) .get("recordCount", 0))

    return run_safely(action, default_return=0, error_message=f"Failed to get row count for {table_name}", )


def database_exists(database_name, region="us-east-1", client=None):
    """Return True when a Glue database exists."""
    databases = list_glue_databases(region=region, client=client)
    return database_name in databases


def table_exists(database_name, table_name, region="us-east-1", client=None):
    """Return True when a Glue table exists in a database."""
    tables = list_glue_tables(database_name, region=region, client=client)
    return table_name in tables
