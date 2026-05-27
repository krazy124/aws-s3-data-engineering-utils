import logging

import boto3
import streamlit as st
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

_GLUE_CLIENT_CACHE = {}


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

        response = glue_client.get_tables(
            DatabaseName=database_name,
        )

        return [
            table["Name"]
            for table in response.get("TableList", [])
        ]

    return run_safely(
        action,
        default_return=[],
        error_message=f"Failed to list Glue tables in database {database_name}",
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


if __name__ == "__main__":
    print(list_glue_databases())
