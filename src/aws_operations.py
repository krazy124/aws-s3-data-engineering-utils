"""
AWS S3 utility functions built with boto3.

This module provides helper functions for common Amazon S3 operations,
including bucket creation, file uploads/downloads, object listing,
and object deletion.

Intended for AWS learning, experimentation, and reusable cloud storage workflows.

Current working module.

Future split plan:
- s3_bucket_operations.py
- s3_file_operations.py
- s3_folder_operations.py
- s3_data_operations.py
"""

import logging
import os
from io import StringIO

import boto3
import pandas as pd
import time
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError,PartialCredentialsError
import streamlit as st

def get_s3_client():
    # 1. Try normal local / environment / AWS config credentials
    try:
        client = boto3.client("s3")
        client.list_buckets()
        return client
    except (NoCredentialsError, PartialCredentialsError):
        pass
    except Exception:
        pass

    # 2. Try Streamlit secrets
    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
            region_name=st.secrets.get("AWS_DEFAULT_REGION", "us-east-1")
        )
        client.list_buckets()
        return client
    except Exception:
        pass

    # 3. Manual credential form
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
        st.rerun()

    if "aws_access_key_id" in st.session_state:
        return boto3.client(
            "s3",
            aws_access_key_id=st.session_state["aws_access_key_id"],
            aws_secret_access_key=st.session_state["aws_secret_access_key"],
            region_name=st.session_state["aws_region"]
        )

    return None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_s3_client(region="us-east-1"):
    """Create and return an S3 client."""
    return boto3.client("s3", region_name=region)

def create_bucket(bucket_name, region="us-east-1"):
    """Create an S3 bucket in a specified region."""
    try:
        s3_client = get_s3_client(region)
        bucket_config = {}

        if region != "us-east-1":
            bucket_config["CreateBucketConfiguration"] = {
                "LocationConstraint": region
            }

        s3_client.create_bucket(Bucket=bucket_name, **bucket_config)
        logging.info(f'Bucket "{bucket_name}" created successfully.')
        return True

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return False

    except ClientError as e:
        logging.error(f'Failed to create bucket "{bucket_name}": {e}')
        return False


def bucket_list(region="us-east-1"):
    """List all S3 buckets in the account."""
    try:
        s3_client = get_s3_client(region)
        response = s3_client.list_buckets()
        return [bucket["Name"] for bucket in response["Buckets"]]

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return []

    except ClientError as e:
        logging.error(f"Failed to list buckets: {e}")
        return []


def delete_bucket(bucket_name, region="us-east-1"):
    """Delete an S3 bucket."""
    try:
        s3_client = get_s3_client(region)
        s3_client.delete_bucket(Bucket=bucket_name)
        logging.info(f'Bucket "{bucket_name}" deleted successfully.')
        return True

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return False

    except ClientError as e:
        logging.error(f'Failed to delete bucket "{bucket_name}": {e}')
        return False


def upload_file(bucket_name, file_name, object_name=None, region="us-east-1"):
    """Upload a file to an S3 bucket."""
    if object_name is None:
        object_name = file_name

    try:
        s3_client = get_s3_client(region)
        s3_client.upload_file(file_name, bucket_name, object_name)
        logging.info(
            f'Uploaded "{file_name}" to "{bucket_name}/{object_name}".'
        )
        return True

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return False

    except ClientError as e:
        logging.error(f'Failed to upload "{file_name}": {e}')
        return False


def download_file(bucket_name, object_name, file_name=None, region="us-east-1"):
    """Download a file from an S3 bucket."""
    if file_name is None:
        file_name = object_name

    try:
        s3_client = get_s3_client(region)
        s3_client.download_file(bucket_name, object_name, file_name)
        logging.info(
            f'Downloaded "{bucket_name}/{object_name}" to "{file_name}".'
        )
        return True

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return False

    except ClientError as e:
        logging.error(f'Failed to download "{object_name}": {e}')
        return False


def delete_file(bucket_name, object_name, region="us-east-1"):
    """Delete a file from an S3 bucket."""
    try:
        s3_client = get_s3_client(region)
        s3_client.delete_object(Bucket=bucket_name, Key=object_name)
        logging.info(f'Deleted "{bucket_name}/{object_name}".')
        return True

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return False

    except ClientError as e:
        logging.error(f'Failed to delete "{object_name}": {e}')
        return False


def list_files(bucket_name, region="us-east-1"):
    """List files in an S3 bucket."""
    try:
        s3_client = get_s3_client(region)
        response = s3_client.list_objects_v2(Bucket=bucket_name)

        if "Contents" in response:
            return [obj["Key"] for obj in response["Contents"]]

        return []

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return []

    except ClientError as e:
        logging.error(f'Failed to list files in "{bucket_name}": {e}')
        return []


def list_folders(bucket_name, prefix="", region="us-east-1"):
    """List folder-like prefixes in an S3 bucket."""
    try:
        s3_client = get_s3_client(region)

        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter="/"
        )

        folders = []

        for item in response.get("CommonPrefixes", []):
            folders.append(item["Prefix"])

        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/") and key not in folders:
                folders.append(key)

        return folders

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return []

    except ClientError as e:
        logging.error(f'Failed to list folders in "{bucket_name}": {e}')
        return []


def create_presigned_url(bucket_name, object_name, region_name, expiration=3600):
    """Generate a presigned URL to share an S3 object."""
    try:
        s3_client = boto3.client(
            "s3",
            region_name=region_name,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "virtual"},
            ),
        )

        response = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_name},
            ExpiresIn=expiration,
        )

        logging.info(
            f'Generated presigned URL for "{bucket_name}/{object_name}".'
        )
        return response

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return None

    except ClientError as e:
        logging.error(
            f'Failed to generate presigned URL for "{object_name}": {e}'
        )
        return None


def bucket_exists(bucket_name, region="us-east-1"):
    """Check if a bucket exists."""
    try:
        s3_client = get_s3_client(region)
        s3_client.head_bucket(Bucket=bucket_name)
        return True

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return False

    except ClientError as e:
        logging.error(f'Bucket check failed for "{bucket_name}": {e}')
        return False


def file_exists(bucket_name, object_name, region="us-east-1"):
    """Check if a file exists in a bucket."""
    try:
        s3_client = get_s3_client(region)
        s3_client.head_object(Bucket=bucket_name, Key=object_name)
        return True

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return False

    except ClientError as e:
        logging.error(
            f'File check failed for "{bucket_name}/{object_name}": {e}'
        )
        return False


def create_folder(bucket_name, folder_name, region="us-east-1"):
    """Create a folder-like prefix in an S3 bucket."""
    if not folder_name.endswith("/"):
        folder_name += "/"

    try:
        s3_client = get_s3_client(region)
        s3_client.put_object(Bucket=bucket_name, Key=folder_name)
        logging.info(f'Created folder "{bucket_name}/{folder_name}".')
        return True

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return False

    except ClientError as e:
        logging.error(f'Failed to create folder "{folder_name}": {e}')
        return False


def upload_folder(bucket_name, folder_name, local_folder_path, region="us-east-1"):
    """Upload a local folder to an S3 bucket."""
    if not folder_name.endswith("/"):
        folder_name += "/"

    try:
        s3_client = get_s3_client(region)

        for root, _, files in os.walk(local_folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)

                relative_path = os.path.relpath(
                    local_file_path,
                    local_folder_path
                )

                s3_object_name = os.path.join(
                    folder_name,
                    relative_path
                ).replace("\\", "/")

                s3_client.upload_file(
                    local_file_path,
                    bucket_name,
                    s3_object_name
                )

        logging.info(
            f'Uploaded folder "{local_folder_path}" '
            f'to "{bucket_name}/{folder_name}".'
        )
        return True

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return False

    except ClientError as e:
        logging.error(f'Failed to upload folder "{local_folder_path}": {e}')
        return False


def download_folder(bucket_name, folder_name, local_folder_path, region="us-east-1"):
    """Download a folder-like prefix from S3 to a local path."""
    if not folder_name.endswith("/"):
        folder_name += "/"

    try:
        s3_client = get_s3_client(region)

        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_name
        )

        for obj in response.get("Contents", []):
            s3_object_name = obj["Key"]

            if s3_object_name.endswith("/"):
                continue

            relative_path = os.path.relpath(s3_object_name, folder_name)
            local_file_path = os.path.join(local_folder_path, relative_path)

            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            s3_client.download_file(
                bucket_name,
                s3_object_name,
                local_file_path
            )

        logging.info(
            f'Downloaded "{bucket_name}/{folder_name}" '
            f'to "{local_folder_path}".'
        )
        return True

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return False

    except ClientError as e:
        logging.error(f'Failed to download folder "{folder_name}": {e}')
        return False


def copy_file(
    source_bucket_name,
    source_object_name,
    dest_bucket_name,
    dest_object_name=None,
    region="us-east-1",
):
    """Copy a file from one S3 bucket to another."""
    if dest_object_name is None:
        dest_object_name = source_object_name

    copy_source = {
        "Bucket": source_bucket_name,
        "Key": source_object_name,
    }

    try:
        s3_client = get_s3_client(region)

        s3_client.copy(
            copy_source,
            dest_bucket_name,
            dest_object_name
        )

        logging.info(
            f'Copied "{source_bucket_name}/{source_object_name}" '
            f'to "{dest_bucket_name}/{dest_object_name}".'
        )
        return True

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return False

    except ClientError as e:
        logging.error(f'Failed to copy "{source_object_name}": {e}')
        return False


def move_file(
    source_bucket_name,
    source_object_name,
    dest_bucket_name,
    dest_object_name=None,
    region="us-east-1",
):
    """Move a file from one S3 bucket to another."""
    if dest_object_name is None:
        dest_object_name = source_object_name

    copied = copy_file(
        source_bucket_name,
        source_object_name,
        dest_bucket_name,
        dest_object_name,
        region,
    )

    if copied:
        return delete_file(source_bucket_name, source_object_name, region)

    return False


def get_file_url(bucket_name, object_name, region_name):
    """Get the public URL of a file in a bucket."""
    return f"https://{bucket_name}.s3.{region_name}.amazonaws.com/{object_name}"


def empty_bucket(bucket_name, region="us-east-1"):
    """Empty all files from a bucket."""
    try:
        s3_client = get_s3_client(region)
        response = s3_client.list_objects_v2(Bucket=bucket_name)

        for obj in response.get("Contents", []):
            s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])

        logging.info(f'Emptied bucket "{bucket_name}".')
        return True

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return False

    except ClientError as e:
        logging.error(f'Failed to empty bucket "{bucket_name}": {e}')
        return False


def read_csv_from_s3(bucket_name, object_name, region="us-east-1"):
    """Read a CSV file from S3 into a pandas DataFrame."""
    try:
        s3_client = get_s3_client(region)

        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=object_name
        )

        csv_content = response["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(csv_content))

        logging.info(f'Read CSV "{bucket_name}/{object_name}" into DataFrame.')
        return df

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return None

    except ClientError as e:
        logging.error(f'Failed to read CSV "{object_name}" from S3: {e}')
        return None


def get_file_metadata(bucket_name, object_name, region="us-east-1"):
    """Get metadata of a file in a bucket."""
    try:
        s3_client = get_s3_client(region)

        response = s3_client.head_object(
            Bucket=bucket_name,
            Key=object_name
        )

        return response["Metadata"]

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return None

    except ClientError as e:
        logging.error(f'Failed to get metadata for "{object_name}": {e}')
        return None


def get_file_size(bucket_name, object_name, region="us-east-1"):
    """Get the size of a file in a bucket."""
    try:
        s3_client = get_s3_client(region)

        response = s3_client.head_object(
            Bucket=bucket_name,
            Key=object_name
        )

        return response["ContentLength"]

    except NoCredentialsError:
        logging.error("AWS credentials not configured.")
        return None

    except ClientError as e:
        logging.error(f'Failed to get file size for "{object_name}": {e}')
        return None


def main():
    """
        Safe test entry point for aws_operations.py.

        aws_operations.py is not intended to be run directly.
        This main function serves as a placeholder for quick tests
        or demonstrations when running this file manually.
    """

    print("aws_operations.py loaded successfully.")
    print("Reusable AWS S3 utility module.")
    print("Import this file into another script or Streamlit app.")


if __name__ == "__main__":
    main()
