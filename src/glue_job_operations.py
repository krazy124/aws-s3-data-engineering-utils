"""AWS Glue job operations."""

import logging

from aws_clients import get_active_glue_client, run_safely

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


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
