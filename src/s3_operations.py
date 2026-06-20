"""Reusable S3 bucket, folder, and file operations."""

import logging
import os
import mimetypes
from datetime import datetime

from aws_clients import get_active_s3_client, run_safely

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def normalize_prefix(prefix):
    if not prefix:
        return ""

    prefix = prefix.replace("\\", "/")

    if not prefix.endswith("/"):
        prefix += "/"

    return prefix


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

    return run_safely(action, default_return=False, error_message=f"Failed to create bucket {bucket_name}", )


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


def list_folders(bucket_name, prefix="", region="us-east-1", client=None):
    def action():
        s3_client = get_active_s3_client(region, client)

        prefix_clean = normalize_prefix(prefix)

        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix_clean,
            Delimiter="/",
        )

        folders = []

        for folder in response.get("CommonPrefixes", []):
            folders.append(folder["Prefix"])

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


def upload_folder(bucket_name, folder_path, object_prefix=None, region="us-east-1", client=None, ):
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


def upload_file(bucket_name, file_path, object_name=None, content_type=None, region="us-east-1", client=None, ):
    if object_name is None:
        object_name = os.path.basename(file_path)

    object_name = object_name.replace("\\", "/")

    if content_type is None:
        content_type, _ = mimetypes.guess_type(file_path)

    def action():
        s3_client = get_active_s3_client(region, client)

        extra_args = {}

        if content_type:
            extra_args["ContentType"] = content_type

        if extra_args:
            s3_client.upload_file(file_path, bucket_name, object_name, ExtraArgs=extra_args, )
        else:
            s3_client.upload_file(file_path, bucket_name, object_name, )
        return True

    return run_safely(action, default_return=False, error_message=f"Failed to upload {file_path} to bucket {bucket_name}", )


def download_folder(bucket_name, object_prefix, folder_path, region="us-east-1", client=None, ):
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


def download_file(bucket_name, object_name, file_path=None, region="us-east-1", client=None, ):
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


def move_folder(source_bucket, source_prefix, dest_bucket, dest_prefix=None, region="us-east-1", client=None, ):
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


def move_file(source_bucket, source_object, dest_bucket, dest_object=None, region="us-east-1", client=None, ):
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


def copy_folder(source_bucket, source_prefix, dest_bucket, dest_prefix=None, region="us-east-1", client=None, ):
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


def copy_file(source_bucket, source_object, dest_bucket, dest_object=None, region="us-east-1", client=None, ):
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


def write_dataframe_to_s3_parquet(df, bucket_name: str, object_prefix: str, mode: str = "overwrite", partition_by: list = None):
    """Write a Spark DataFrame to S3 as Parquet.

    This expects a PySpark DataFrame. For local pandas DataFrames, convert to
    Spark first or use a CSV helper.
    """
    object_prefix_clean = normalize_prefix(object_prefix)
    s3_path = f"s3://{bucket_name}/{object_prefix_clean}"

    def action():
        writer = df.write.mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.parquet(s3_path)
        return s3_path

    return run_safely(
        action,
        default_return=None,
        error_message=f"Failed to write DataFrame to {s3_path} as Parquet",
    )


def build_upload_extra_args(content_type=None, cache_control=None, metadata=None):
    extra_args = {}

    if content_type:
        extra_args["ContentType"] = content_type

    if cache_control:
        extra_args["CacheControl"] = cache_control

    if metadata:
        extra_args["Metadata"] = metadata

    return extra_args


if __name__ == "__main__":
    bucket_name = "monsterforge-portfolio-site"
    region = "us-east-1"
    file_path = r"C:\Users\willi\OneDrive\Desktop\aws-s3-data-engineering-utils\index.html"
    results = upload_file(bucket_name, file_path, object_name=None, region=region, client=None, )
    print(results)
