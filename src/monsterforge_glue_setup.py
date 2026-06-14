# monsterforge_glue_setup.py
# Automates MonsterForge Glue database, crawler creation, crawler runs, and table verification.

import logging

from glue_operations import (
    get_active_glue_client,
    run_safely,
    list_glue_databases,
    list_glue_tables,
    create_glue_crawler,
    get_crawler_info,
    run_crawler_workflow,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

REGION = "us-east-1"

DATABASE_NAME = "monsterforge_data_lake"

BUCKET_NAME = "wlmdatawizard-monsterforge-873851887650"

GLUE_ROLE_ARN = (
    "arn:aws:iam::873851887650:role/service-role/"
    "AWSGlueServiceRole-OlistCrawler"
)

CRAWLERS = [
    {
        "crawler_name": "monsterforge-clean-crawler",
        "s3_target_path": f"s3://{BUCKET_NAME}/clean/monsters/",
        "description": "MonsterForge clean monsters crawler",
    },
    {
        "crawler_name": "monsterforge-quarantine-crawler",
        "s3_target_path": f"s3://{BUCKET_NAME}/quarantine/monsters/",
        "description": "MonsterForge quarantine monsters crawler",
    },
    {
        "crawler_name": "monsterforge-reports-crawler",
        "s3_target_path": f"s3://{BUCKET_NAME}/reports/",
        "description": "MonsterForge quality reports crawler",
    },
]


def create_glue_database_if_missing(database_name: str, region: str = REGION):
    """Create a Glue database if it does not already exist."""

    def action():
        glue_client = get_active_glue_client(region)

        existing_databases = list_glue_databases(region=region)

        if database_name in existing_databases:
            print(f"Database already exists: {database_name}")
            return True

        glue_client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": "MonsterForge Industries Data Lake",
            }
        )

        print(f"Created Glue database: {database_name}")
        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to create Glue database: {database_name}",
    )


def crawler_exists(crawler_name: str, region: str = REGION):
    """Check whether a Glue crawler already exists."""
    crawler_info = get_crawler_info(crawler_name, region=region)
    return bool(crawler_info)


def create_crawler_if_missing(crawler_config: dict, region: str = REGION):
    """Create a Glue crawler if it does not already exist."""

    crawler_name = crawler_config["crawler_name"]

    if crawler_exists(crawler_name, region=region):
        print(f"Crawler already exists: {crawler_name}")
        return True

    response = create_glue_crawler(
        crawler_name=crawler_name,
        database_name=DATABASE_NAME,
        role_arn=GLUE_ROLE_ARN,
        s3_target_path=crawler_config["s3_target_path"],
        description=crawler_config["description"],
        region=region,
    )

    if response is not None:
        print(f"Created crawler: {crawler_name}")
        return True

    print(f"Failed to create crawler: {crawler_name}")
    return False


def create_all_monsterforge_crawlers(region: str = REGION):
    """Create all MonsterForge crawlers if missing."""

    results = {}

    for crawler_config in CRAWLERS:
        crawler_name = crawler_config["crawler_name"]

        print("\n" + "=" * 70)
        print(f"Creating / Verifying Crawler: {crawler_name}")
        print("=" * 70)

        results[crawler_name] = create_crawler_if_missing(
            crawler_config,
            region=region,
        )

    return results


def run_all_monsterforge_crawlers(region: str = REGION):
    """Run all MonsterForge crawlers and wait for completion."""

    results = {}

    for crawler_config in CRAWLERS:
        crawler_name = crawler_config["crawler_name"]

        print("\n" + "=" * 70)
        print(f"Running Crawler: {crawler_name}")
        print("=" * 70)

        results[crawler_name] = run_crawler_workflow(
            crawler_name,
            region=region,
        )

    return results


def print_monsterforge_tables(region: str = REGION):
    """Print tables in the MonsterForge Glue database."""

    tables = list_glue_tables(
        DATABASE_NAME,
        region=region,
    )

    print("\n" + "=" * 70)
    print(f"TABLES IN DATABASE: {DATABASE_NAME}")
    print("=" * 70)

    if not tables:
        print("No tables found.")
        return []

    for index, table in enumerate(tables, start=1):
        print(f"{index}. {table}")

    return tables


def run_monsterforge_glue_setup():
    """Full MonsterForge Glue setup workflow."""

    print("\n" + "=" * 70)
    print("MONSTERFORGE GLUE SETUP")
    print("=" * 70)

    database_ready = create_glue_database_if_missing(DATABASE_NAME)

    if not database_ready:
        print("Database setup failed. Stopping workflow.")
        return False

    crawler_creation_results = create_all_monsterforge_crawlers()

    if not all(crawler_creation_results.values()):
        print("One or more crawlers failed to create. Stopping workflow.")
        return False

    crawler_run_results = run_all_monsterforge_crawlers()

    if not all(crawler_run_results.values()):
        print("One or more crawlers failed to run successfully.")
        return False

    print_monsterforge_tables()

    print("\n" + "=" * 70)
    print("MONSTERFORGE GLUE SETUP COMPLETE")
    print("=" * 70)

    return True


if __name__ == "__main__":
    run_monsterforge_glue_setup()
