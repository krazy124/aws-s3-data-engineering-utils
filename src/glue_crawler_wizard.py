"""Interactive CLI wizard for creating AWS Glue crawlers."""

import logging

import boto3
from botocore.exceptions import ClientError

from aws_clients import get_active_glue_client
from s3_operations import list_buckets, list_folders
from glue_catalog_operations import list_glue_databases
from glue_crawler_operations import list_glue_crawlers, create_glue_crawler
from glue_diagnostics import show_all_crawler_details

REGION = "us-east-1"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Backward-compatible alias for the old wizard function name.
def bucket_list(region="us-east-1"):
    return list_buckets(region=region)


def choose_from_list(title, options):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)

    for index, option in enumerate(options, start=1):
        print(f"{index}. {option}")

    while True:
        choice = input("\nChoose a number: ").strip()

        if choice.isdigit():
            choice_index = int(choice) - 1

            if 0 <= choice_index < len(options):
                return options[choice_index]

        print("Invalid choice. Try again.")


def create_database_if_missing(database_name):
    glue_client = get_active_glue_client(REGION)

    existing_databases = list_glue_databases(region=REGION)

    if database_name in existing_databases:
        print(f"Database already exists: {database_name}")
        return True

    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": f"Created by Glue Crawler Wizard: {database_name}",
            }
        )

        print(f"Created database: {database_name}")
        return True

    except ClientError as e:
        print(f"Failed to create database: {e}")
        return False


def choose_database():
    databases = list_glue_databases(region=REGION)

    options = databases + ["Create new database"]

    selected = choose_from_list("Choose Glue Database", options)

    if selected == "Create new database":
        database_name = input("\nEnter new database name: ").strip()

        if create_database_if_missing(database_name):
            return database_name

        return None

    return selected


def list_glue_role_arns():
    iam_client = boto3.client("iam")

    role_arns = []

    try:
        paginator = iam_client.get_paginator("list_roles")

        for page in paginator.paginate():
            for role in page.get("Roles", []):
                role_name = role.get("RoleName", "")
                role_arn = role.get("Arn", "")

                if "glue" in role_name.lower():
                    role_arns.append(role_arn)

    except ClientError as e:
        print(f"Could not list IAM roles: {e}")
        return []

    return role_arns


def choose_glue_role():
    role_arns = list_glue_role_arns()

    if role_arns:
        role_arns.append("Enter role ARN manually")

        selected = choose_from_list("Choose Glue IAM Role", role_arns)

        if selected != "Enter role ARN manually":
            return selected

    return input("\nPaste full Glue role ARN: ").strip()


def choose_bucket():
    buckets = bucket_list(region=REGION)

    if not buckets:
        print("No buckets found.")
        return None

    return choose_from_list("Choose S3 Bucket", buckets)


def choose_s3_target_path(bucket_name):
    current_prefix = ""

    while True:
        folders = list_folders(
            bucket_name=bucket_name,
            prefix=current_prefix,
            region=REGION,
        )

        print("\n" + "=" * 70)
        print(f"Current S3 Path: s3://{bucket_name}/{current_prefix}")
        print("=" * 70)

        options = []

        if current_prefix:
            options.append("Use this folder as crawler target")
            options.append("Go up one level")
        else:
            options.append("Use bucket root as crawler target")

        options.extend(folders)

        for index, option in enumerate(options, start=1):
            print(f"{index}. {option}")

        choice = input("\nChoose a number: ").strip()

        if not choice.isdigit():
            print("Invalid choice. Try again.")
            continue

        choice_index = int(choice) - 1

        if not 0 <= choice_index < len(options):
            print("Invalid choice. Try again.")
            continue

        selected = options[choice_index]

        if selected == "Use this folder as crawler target":
            return f"s3://{bucket_name}/{current_prefix}"

        if selected == "Use bucket root as crawler target":
            return f"s3://{bucket_name}/"

        if selected == "Go up one level":
            current_prefix = "/".join(current_prefix.rstrip("/").split("/")[:-1])

            if current_prefix:
                current_prefix += "/"

            continue

        current_prefix = selected


def crawler_exists(crawler_name):
    crawlers = list_glue_crawlers(region=REGION)
    return crawler_name in crawlers


def main_menu():
    while True:
        print("\n" + "=" * 70)
        print("AWS GLUE CRAWLER WIZARD")
        print("=" * 70)
        print("1. View Existing Crawlers")
        print("2. Create New Crawler")
        print("3. Exit")

        choice = input("\nChoose an option: ").strip()

        if choice == "1":
            show_all_crawler_details(region=REGION)

        elif choice == "2":
            run_crawler_creation_wizard()

        elif choice == "3":
            print("Exiting Glue Crawler Wizard.")
            break

        else:
            print("Invalid choice. Try again.")


def run_crawler_creation_wizard():
    print("\n" + "=" * 70)
    print("AWS GLUE CRAWLER CREATION WIZARD")
    print("=" * 70)

    bucket_name = choose_bucket()

    if not bucket_name:
        print("No bucket selected. Stopping.")
        return False

    s3_target_path = choose_s3_target_path(bucket_name)

    database_name = choose_database()

    if not database_name:
        print("No database selected. Stopping.")
        return False

    role_arn = choose_glue_role()

    crawler_name = input("\nEnter crawler name: ").strip()

    if crawler_exists(crawler_name):
        print(f"Crawler already exists: {crawler_name}")
        return False

    description = input("\nEnter crawler description: ").strip()

    print("\n" + "=" * 70)
    print("REVIEW CRAWLER SETTINGS")
    print("=" * 70)
    print(f"Crawler Name:   {crawler_name}")
    print(f"Database:       {database_name}")
    print(f"Role ARN:       {role_arn}")
    print(f"S3 Target Path: {s3_target_path}")
    print(f"Description:    {description}")

    confirm = input("\nCreate this crawler? (y/n): ").strip().lower()

    if confirm != "y":
        print("Crawler creation cancelled.")
        return False

    response = create_glue_crawler(
        crawler_name=crawler_name,
        database_name=database_name,
        role_arn=role_arn,
        s3_target_path=s3_target_path,
        description=description,
        region=REGION,
    )

    if response is not None:
        print("\nCrawler created successfully.")
        return True

    print("\nCrawler creation failed.")
    return False


if __name__ == "__main__":
    main_menu()
