import logging
import os

import boto3
import streamlit as st
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

_S3_CLIENT_CACHE = {}


def normalize_prefix(prefix):
    if not prefix:
        return ""

    prefix = prefix.replace("\\", "/")

    if not prefix.endswith("/"):
        prefix += "/"

    return prefix


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


def create_bucket(bucket_name, region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)

        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to create bucket {bucket_name}",
    )


def create_folder(bucket_name, folder_name, region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)
        folder_name_clean = normalize_prefix(folder_name)

        s3_client.put_object(Bucket=bucket_name, Key=folder_name_clean)

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to create folder {folder_name} in bucket {bucket_name}",
    )


def list_buckets(region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)
        response = s3_client.list_buckets()
        return [bucket["Name"] for bucket in response["Buckets"]]

    return run_safely(
        action,
        default_return=[],
        error_message="Failed to list buckets",
    )


def list_folders(bucket_name, region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)
        response = s3_client.list_objects_v2(Bucket=bucket_name)

        folders = set()

        for obj in response.get("Contents", []):
            key = obj["Key"]

            if "/" in key:
                folder_path = "/".join(key.split("/")[:-1]) + "/"
                folders.add(folder_path)

        return sorted(folders)

    return run_safely(
        action,
        default_return=[],
        error_message=f"Failed to list folders in bucket {bucket_name}",
    )


def list_files(bucket_name, region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        return [obj["Key"] for obj in response.get("Contents", [])]

    return run_safely(
        action,
        default_return=[],
        error_message=f"Failed to list objects in bucket {bucket_name}",
    )


def exists_bucket(bucket_name, region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)
        buckets = list_buckets(region=region, client=s3_client)
        return bucket_name in buckets

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to check if bucket {bucket_name} exists",
    )


def exists_folder(bucket_name, folder_name, region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)
        folder_name_clean = normalize_prefix(folder_name)
        folders = list_folders(bucket_name, region=region, client=s3_client)
        return folder_name_clean in folders

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to check if folder {folder_name} exists in bucket {bucket_name}",
    )


def exists_file(bucket_name, object_name, region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)
        files = list_files(bucket_name, region=region, client=s3_client)
        return object_name in files

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to check if {object_name} exists in bucket {bucket_name}",
    )


def delete_bucket(bucket_name, region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)
        s3_client.delete_bucket(Bucket=bucket_name)
        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to delete bucket {bucket_name}",
    )


def delete_folder(bucket_name, object_prefix, region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)
        object_prefix_clean = normalize_prefix(object_prefix)

        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=object_prefix_clean,
        )

        for obj in response.get("Contents", []):
            deleted = delete_file(
                bucket_name,
                obj["Key"],
                region=region,
                client=s3_client,
            )

            if not deleted:
                return False

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to delete folder {object_prefix} from bucket {bucket_name}",
    )


def delete_file(bucket_name, object_name, region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)
        s3_client.delete_object(Bucket=bucket_name, Key=object_name)
        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to delete {object_name} from bucket {bucket_name}",
    )


def empty_bucket(bucket_name, region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)
        response = s3_client.list_objects_v2(Bucket=bucket_name)

        for obj in response.get("Contents", []):
            s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to empty bucket {bucket_name}",
    )


def empty_folder(bucket_name, object_prefix, region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)
        object_prefix_clean = normalize_prefix(object_prefix)

        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=object_prefix_clean,
        )

        for obj in response.get("Contents", []):
            s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to empty folder {object_prefix} from bucket {bucket_name}",
    )


def upload_folder(
    bucket_name,
    folder_path,
    object_prefix=None,
    region="us-east-1",
    client=None,
):
    def action():
        s3_client = get_active_s3_client(region, client)
        object_prefix_clean = normalize_prefix(object_prefix)

        for root, _, files in os.walk(folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)

                relative_path = os.path.relpath(
                    local_file_path,
                    folder_path,
                )

                if object_prefix_clean:
                    object_name = os.path.join(
                        object_prefix_clean,
                        relative_path,
                    )
                else:
                    object_name = relative_path

                object_name = object_name.replace("\\", "/")

                uploaded = upload_file(
                    bucket_name,
                    local_file_path,
                    object_name,
                    region=region,
                    client=s3_client,
                )

                if not uploaded:
                    return False

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to upload folder {folder_path} to bucket {bucket_name}",
    )


def upload_file(
    bucket_name,
    file_path,
    object_name=None,
    region="us-east-1",
    client=None,
):
    if object_name is None:
        object_name = os.path.basename(file_path)

    object_name = object_name.replace("\\", "/")

    def action():
        s3_client = get_active_s3_client(region, client)

        s3_client.upload_file(
            file_path,
            bucket_name,
            object_name,
        )

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to upload {file_path} to bucket {bucket_name}",
    )


def download_folder(
    bucket_name,
    object_prefix,
    folder_path,
    region="us-east-1",
    client=None,
):
    def action():
        s3_client = get_active_s3_client(region, client)
        object_prefix_clean = normalize_prefix(object_prefix)

        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=object_prefix_clean,
        )

        for obj in response.get("Contents", []):
            object_name = obj["Key"]

            local_file_path = os.path.join(
                folder_path,
                os.path.relpath(object_name, object_prefix_clean),
            )

            downloaded = download_file(
                bucket_name,
                object_name,
                local_file_path,
                region=region,
                client=s3_client,
            )

            if not downloaded:
                return False

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to download folder {object_prefix} from bucket {bucket_name} to {folder_path}",
    )


def download_file(
    bucket_name,
    object_name,
    file_path=None,
    region="us-east-1",
    client=None,
):
    if file_path is None:
        file_path = object_name

    def action():
        folder_name = os.path.dirname(file_path)

        if folder_name:
            os.makedirs(folder_name, exist_ok=True)

        s3_client = get_active_s3_client(region, client)

        s3_client.download_file(
            bucket_name,
            object_name,
            file_path,
        )

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to download {object_name} from bucket {bucket_name}",
    )


def move_folder(
    source_bucket,
    source_prefix,
    dest_bucket,
    dest_prefix=None,
    region="us-east-1",
    client=None,
):
    def action():
        s3_client = get_active_s3_client(region, client)

        source_prefix_clean = normalize_prefix(source_prefix)
        dest_prefix_clean = normalize_prefix(dest_prefix)

        response = s3_client.list_objects_v2(
            Bucket=source_bucket,
            Prefix=source_prefix_clean,
        )

        for obj in response.get("Contents", []):
            source_object = obj["Key"]

            if dest_prefix_clean:
                dest_object = os.path.join(
                    dest_prefix_clean,
                    os.path.relpath(source_object, source_prefix_clean),
                ).replace("\\", "/")
            else:
                dest_object = source_object

            moved = move_file(
                source_bucket,
                source_object,
                dest_bucket,
                dest_object,
                region=region,
                client=s3_client,
            )

            if not moved:
                return False

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=(
            f"Failed to move folder {source_prefix} "
            f"from bucket {source_bucket} "
            f"to bucket {dest_bucket} "
            f"with prefix {dest_prefix}"
        ),
    )


def move_file(
    source_bucket,
    source_object,
    dest_bucket,
    dest_object=None,
    region="us-east-1",
    client=None,
):
    if dest_object is None:
        dest_object = source_object

    def action():
        s3_client = get_active_s3_client(region, client)

        s3_client.copy(
            {"Bucket": source_bucket, "Key": source_object},
            dest_bucket,
            dest_object,
        )

        s3_client.delete_object(
            Bucket=source_bucket,
            Key=source_object,
        )

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to move {source_object} from bucket {source_bucket} to bucket {dest_bucket}",
    )


def copy_folder(
    source_bucket,
    source_prefix,
    dest_bucket,
    dest_prefix=None,
    region="us-east-1",
    client=None,
):
    def action():
        s3_client = get_active_s3_client(region, client)

        source_prefix_clean = normalize_prefix(source_prefix)
        dest_prefix_clean = normalize_prefix(dest_prefix)

        response = s3_client.list_objects_v2(
            Bucket=source_bucket,
            Prefix=source_prefix_clean,
        )

        for obj in response.get("Contents", []):
            source_object = obj["Key"]

            if dest_prefix_clean:
                dest_object = os.path.join(
                    dest_prefix_clean,
                    os.path.relpath(source_object, source_prefix_clean),
                ).replace("\\", "/")
            else:
                dest_object = source_object

            copied = copy_file(
                source_bucket,
                source_object,
                dest_bucket,
                dest_object,
                region=region,
                client=s3_client,
            )

            if not copied:
                return False

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=(
            f"Failed to copy folder {source_prefix} "
            f"from bucket {source_bucket} "
            f"to bucket {dest_bucket} "
            f"with prefix {dest_prefix}"
        ),
    )


def copy_file(
    source_bucket,
    source_object,
    dest_bucket,
    dest_object=None,
    region="us-east-1",
    client=None,
):
    if dest_object is None:
        dest_object = source_object

    def action():
        s3_client = get_active_s3_client(region, client)

        s3_client.copy(
            {"Bucket": source_bucket, "Key": source_object},
            dest_bucket,
            dest_object,
        )

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to copy {source_object} from bucket {source_bucket} to bucket {dest_bucket}",
    )


def list_files_in_prefix(bucket_name, prefix, region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)
        prefix_clean = normalize_prefix(prefix)

        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix_clean,
        )

        return [obj["Key"] for obj in response.get("Contents", [])]

    return run_safely(
        action,
        default_return=[],
        error_message=f"Failed to list files in {prefix} from bucket {bucket_name}",
    )


if __name__ == "__main__":
    print(list_buckets())
