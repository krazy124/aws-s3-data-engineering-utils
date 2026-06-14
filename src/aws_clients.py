"""Shared AWS client creation and safety helpers."""

import logging
import time

import boto3
import streamlit as st
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

_S3_CLIENT_CACHE = {}
_GLUE_CLIENT_CACHE = {}
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


def get_s3_client(region="us-east-1"):
    try:
        client = boto3.client("s3", region_name=region)
        client.list_buckets()

        logging.info(
            f"Connected to AWS using local/default credentials in region {region}"
        )

        return client

    except (NoCredentialsError, PartialCredentialsError) as e:
        logging.warning(f"Local/default AWS credentials not available: {e}")

    except Exception as e:
        logging.error(f"Failed local/default AWS connection attempt: {e}")

    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
            region_name=st.secrets.get("AWS_DEFAULT_REGION", region),
        )

        client.list_buckets()

        logging.info("Connected to AWS using Streamlit secrets credentials")

        return client

    except Exception as e:
        logging.error(f"Failed Streamlit secrets AWS connection attempt: {e}")

    st.warning("AWS credentials were not found. Enter credentials below.")

    with st.form("aws_credentials_form"):
        access_key = st.text_input("AWS Access Key ID")
        secret_key = st.text_input("AWS Secret Access Key", type="password")
        region = st.text_input("AWS Region", value="us-east-1")
        submitted = st.form_submit_button("Connect to AWS")

    if submitted:
        st.session_state["aws_access_key_id"] = access_key
        st.session_state["aws_secret_access_key"] = secret_key
        st.session_state["aws_region"] = region

        _S3_CLIENT_CACHE.clear()

        logging.info("AWS credentials stored in Streamlit session state")

        st.rerun()

    if "aws_access_key_id" in st.session_state:
        try:
            client = boto3.client(
                "s3",
                aws_access_key_id=st.session_state["aws_access_key_id"],
                aws_secret_access_key=st.session_state["aws_secret_access_key"],
                region_name=st.session_state["aws_region"],
            )

            client.list_buckets()

            logging.info("Connected to AWS using session state credentials")

            return client

        except Exception as e:
            logging.error(f"Failed session state AWS connection attempt: {e}")

    logging.error("Unable to establish AWS S3 client connection")

    return None


def get_active_s3_client(region="us-east-1", client=None):
    if client is not None:
        return client

    if region in _S3_CLIENT_CACHE:
        return _S3_CLIENT_CACHE[region]

    new_client = get_s3_client(region)

    if new_client is not None:
        _S3_CLIENT_CACHE[region] = new_client

    return new_client


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
