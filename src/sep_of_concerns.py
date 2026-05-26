import logging
import os

import boto3
import streamlit as st
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_s3_client(region="us-east-1"):
    try:
        client = boto3.client("s3", region_name=region)
        client.list_buckets()
        return client

    except (NoCredentialsError, PartialCredentialsError):
        pass

    except Exception:
        pass

    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
            region_name=st.secrets.get("AWS_DEFAULT_REGION", region),
        )
        client.list_buckets()
        return client

    except Exception:
        pass

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
            region_name=st.session_state["aws_region"],
        )

    return None


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


def create_bucket(bucket_name, region="us-east-1"):
    def action():
        client = get_s3_client(region)
        client.create_bucket(Bucket=bucket_name)
        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to create bucket {bucket_name}",
    )


def create_folder(bucket_name, folder_name, region="us-east-1"):
    def action():
        client = get_s3_client(region)

        if not folder_name.endswith("/"):
            folder_name_key = folder_name + "/"
        else:
            folder_name_key = folder_name

        client.put_object(
            Bucket=bucket_name,
            Key=folder_name_key,
        )

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to create folder {folder_name} in bucket {bucket_name}",
    )


def list_buckets(region="us-east-1"):
    def action():
        client = get_s3_client(region)
        response = client.list_buckets()
        return [bucket["Name"] for bucket in response["Buckets"]]

    return run_safely(
        action,
        default_return=[],
        error_message="Failed to list buckets",
    )


def list_folders(bucket_name, region="us-east-1"):
    def action():
        client = get_s3_client(region)

        response = client.list_objects_v2(Bucket=bucket_name)

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


def list_files(bucket_name, region="us-east-1"):
    def action():
        client = get_s3_client(region)

        response = client.list_objects_v2(Bucket=bucket_name)

        return [obj["Key"] for obj in response.get("Contents", [])]

    return run_safely(
        action,
        default_return=[],
        error_message=f"Failed to list objects in bucket {bucket_name}",
    )


def exists_bucket(bucket_name, region="us-east-1"):
    def action():
        buckets = list_buckets(region)
        return bucket_name in buckets

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to check if bucket {bucket_name} exists",
    )


def exists_folder(bucket_name, folder_name, region="us-east-1"):
    def action():
        folders = list_folders(bucket_name, region)
        return folder_name in folders

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to check if folder {folder_name} exists in bucket {bucket_name}",
    )


def exists_file(bucket_name, object_name, region="us-east-1"):
    def action():
        files = list_files(bucket_name, region)
        return object_name in files

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to check if {object_name} exists in bucket {bucket_name}",
    )


def delete_bucket(bucket_name, region="us-east-1"):
    def action():
        client = get_s3_client(region)

        client.delete_bucket(Bucket=bucket_name)

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to delete bucket {bucket_name}",
    )


def delete_folder(bucket_name, object_prefix, region="us-east-1"):
    def action():
        client = get_s3_client(region)

        response = client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=object_prefix,
        )

        for obj in response.get("Contents", []):
            delete_file(
                bucket_name,
                obj["Key"],
                region,
            )

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to delete folder {object_prefix} from bucket {bucket_name}",
    )


def delete_file(bucket_name, object_name, region="us-east-1"):
    def action():
        client = get_s3_client(region)

        client.delete_object(
            Bucket=bucket_name,
            Key=object_name,
        )

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to delete {object_name} from bucket {bucket_name}",
    )


def empty_bucket(bucket_name, region="us-east-1"):
    def action():
        client = get_s3_client(region)

        response = client.list_objects_v2(Bucket=bucket_name)

        for obj in response.get("Contents", []):
            client.delete_object(
                Bucket=bucket_name,
                Key=obj["Key"],
            )

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to empty bucket {bucket_name}",
    )


def empty_folder(bucket_name, object_prefix, region="us-east-1"):
    def action():
        client = get_s3_client(region)

        response = client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=object_prefix,
        )

        for obj in response.get("Contents", []):
            client.delete_object(
                Bucket=bucket_name,
                Key=obj["Key"],
            )

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to empty folder {object_prefix} from bucket {bucket_name}",
    )


def upload_folder(bucket_name, folder_path, object_prefix=None, region="us-east-1"):
    def action():
        for root, _, files in os.walk(folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)

                relative_path = os.path.relpath(
                    local_file_path,
                    folder_path,
                )

                if object_prefix:
                    object_name = os.path.join(
                        object_prefix,
                        relative_path,
                    )
                else:
                    object_name = relative_path

                object_name = object_name.replace("\\", "/")

                uploaded = upload_file(
                    bucket_name,
                    local_file_path,
                    object_name,
                    region,
                )

                if not uploaded:
                    return False

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to upload folder {folder_path} to bucket {bucket_name}",
    )


def upload_file(bucket_name, file_path, object_name=None, region="us-east-1"):
    if object_name is None:
        object_name = file_path

    def action():
        client = get_s3_client(region)

        client.upload_file(
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


def download_folder(bucket_name, object_prefix, folder_path, region="us-east-1"):
    def action():
        client = get_s3_client(region)

        response = client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=object_prefix,
        )

        for obj in response.get("Contents", []):
            object_name = obj["Key"]

            local_file_path = os.path.join(
                folder_path,
                os.path.relpath(object_name, object_prefix),
            )

            downloaded = download_file(
                bucket_name,
                object_name,
                local_file_path,
                region,
            )

            if not downloaded:
                return False

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to download folder {object_prefix} from bucket {bucket_name} to {folder_path}",
    )


def download_file(bucket_name, object_name, file_path=None, region="us-east-1"):
    if file_path is None:
        file_path = object_name

    def action():
        folder_name = os.path.dirname(file_path)

        if folder_name:
            os.makedirs(
                folder_name,
                exist_ok=True,
            )

        client = get_s3_client(region)

        client.download_file(
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
):
    def action():
        client = get_s3_client(region)

        response = client.list_objects_v2(
            Bucket=source_bucket,
            Prefix=source_prefix,
        )

        for obj in response.get("Contents", []):
            source_object = obj["Key"]

            if dest_prefix:
                dest_object = os.path.join(
                    dest_prefix,
                    os.path.relpath(source_object, source_prefix),
                ).replace("\\", "/")
            else:
                dest_object = source_object

            moved = move_file(
                source_bucket,
                source_object,
                dest_bucket,
                dest_object,
                region,
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
):
    if dest_object is None:
        dest_object = source_object

    def action():
        client = get_s3_client(region)

        client.copy(
            {"Bucket": source_bucket, "Key": source_object},
            dest_bucket,
            dest_object,
        )

        client.delete_object(
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
):
    def action():
        client = get_s3_client(region)

        response = client.list_objects_v2(
            Bucket=source_bucket,
            Prefix=source_prefix,
        )

        for obj in response.get("Contents", []):
            source_object = obj["Key"]

            if dest_prefix:
                dest_object = os.path.join(
                    dest_prefix,
                    os.path.relpath(source_object, source_prefix),
                ).replace("\\", "/")
            else:
                dest_object = source_object

            copied = copy_file(
                source_bucket,
                source_object,
                dest_bucket,
                dest_object,
                region,
            )

            if not copied:
                return False

        return True

    return run_safely(
        action,
        default_return=False,
        error_message=f"Failed to copy folder {source_prefix} from bucket {source_bucket} to bucket {dest_bucket} with prefix {dest_prefix}",
    )


def copy_file(
    source_bucket,
    source_object,
    dest_bucket,
    dest_object=None,
    region="us-east-1",
):
    if dest_object is None:
        dest_object = source_object

    def action():
        client = get_s3_client(region)

        client.copy(
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


if __name__ == "__main__":
    print(list_buckets())
