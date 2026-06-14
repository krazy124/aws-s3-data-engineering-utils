"""AWS Glue crawler operations."""

import logging
import time

from aws_clients import get_active_glue_client, run_safely

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


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


def get_crawler_details(crawler_name, region="us-east-1", client=None):

    def action():
        glue_client = get_active_glue_client(region, client)

        response = glue_client.get_crawler(
            Name=crawler_name
        )

        crawler = response["Crawler"]

        return {
            "name": crawler["Name"],
            "database": crawler["DatabaseName"],
            "role": crawler["Role"],
            "targets": crawler["Targets"]["S3Targets"],
            "state": crawler["State"],
        }

    return run_safely(
        action,
        default_return=None,
        error_message=f"Failed to get crawler details: {crawler_name}",
    )


def get_crawler_info(crawler_name, region="us-east-1", client=None):
    def action():
        glue_client = get_active_glue_client(region, client)

        response = glue_client.get_crawler(
            Name=crawler_name,
        )

        return response.get("Crawler", {})

    return run_safely(
        action,
        default_return={},
        error_message=f"Failed to get crawler info for {crawler_name}",
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
