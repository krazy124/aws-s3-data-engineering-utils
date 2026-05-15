"""
AWS S3 utility functions built with boto3.

This module provides helper functions for common Amazon S3 operations,
including bucket creation, file uploads/downloads, object listing,
and object deletion.

Intended for AWS learning, experimentation, and reusable cloud storage workflows.
"""

import logging
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config


s3_client = boto3.client('s3')


def create_bucket(bucket_name, region='us-east-1'):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        bucket_config = {}
        s3_client = boto3.client('s3', region_name=region)
        if region != 'us-east-1':
            bucket_config['CreateBucketConfiguration'] = {
                'LocationConstraint': region}

        s3_client.create_bucket(Bucket=bucket_name, **bucket_config)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def bucket_list():
    """List all S3 buckets in the account

    :return: List of bucket names
    """

    response = s3_client.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    return buckets


def delete_bucket(bucket_name):
    """Delete an S3 bucket

    :param bucket_name: Bucket to delete
    :return: True if bucket deleted, else False
    """

    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(bucket_name, file_name, object_name=None):
    """Upload a file to an S3 bucket

    :param bucket_name: Bucket to upload to
    :param file_name: File to upload
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    if object_name is None:
        object_name = file_name

    try:
        s3_client.upload_file(file_name, bucket_name, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def download_file(bucket_name, object_name, file_name=None):
    """Download a file from an S3 bucket

    :param bucket_name: Bucket to download from
    :param object_name: S3 object name to download
    :param file_name: File name to save as. If not specified then object_name is used
    :return: True if file was downloaded, else False
    """
    if file_name is None:
        file_name = object_name

    try:
        s3_client.download_file(bucket_name, object_name, file_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def delete_file(bucket_name, object_name):
    """Delete a file from an S3 bucket

    :param bucket_name: Bucket to delete from
    :param object_name: S3 object name to delete
    :return: True if file was deleted, else False
    """

    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def list_files(bucket_name):
    """List files in an S3 bucket

    :param bucket_name: Bucket to list files from
    :return: List of file names in the bucket
    """

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents']]
        else:
            return []
    except ClientError as e:
        logging.error(e)
        return []


def create_presigned_url(
    bucket_name, object_name, region_name, expiration=3600
):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param region_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    s3_client = boto3.client(
        's3',
        region_name=region_name,
        config=Config(
            signature_version='s3v4',
            s3={'addressing_style': 'virtual'},
        ),
    )
    try:
        response = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_name},
            ExpiresIn=expiration,
        )
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL
    return response


def bucket_exists(bucket_name):
    """Check if a bucket exists

    :param bucket_name: Name of the bucket to check
    :return: True if the bucket exists, else False
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        logging.error(e)
        return False


def file_exists(bucket_name, object_name):
    """Check if a file exists in a bucket

    :param bucket_name: Name of the bucket to check
    :param object_name: Name of the object to check
    :return: True if the object exists, else False
    """
    try:
        s3_client.head_object(Bucket=bucket_name, Key=object_name)
        return True
    except ClientError as e:
        logging.error(e)
        return False


def create_folder(bucket_name, folder_name):
    """Create a folder in an S3 bucket

    :param bucket_name: Name of the bucket to create the folder in
    :param folder_name: Name of the folder to create (should end with '/')
    :return: True if the folder was created, else False
    """
    if not folder_name.endswith('/'):
        folder_name += '/'

    try:
        s3_client.put_object(Bucket=bucket_name, Key=folder_name)
        return True
    except ClientError as e:
        logging.error(e)
        return False


def upload_folder(bucket_name, folder_name, local_folder_path):
    """Upload a local folder to an S3 bucket

    :param bucket_name: Name of the bucket to upload to
    :param folder_name: Name of the folder in S3 (should end with '/')
    :param local_folder_path: Path to the local folder to upload
    :return: True if the folder was uploaded, else False
    """
    if not folder_name.endswith('/'):
        folder_name += '/'

    try:
        for root, dirs, files in os.walk(local_folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                s3_object_name = os.path.join(
                    folder_name, os.path.relpath(local_file_path, local_folder_path))
                s3_client.upload_file(
                    local_file_path, bucket_name, s3_object_name)
        return True
    except ClientError as e:
        logging.error(e)
        return False


def download_folder(bucket_name, folder_name, local_folder_path):
    """Download a folder from an S3 bucket to a local path

    :param bucket_name: Name of the bucket to download from
    :param folder_name: Name of the folder in S3 (should end with '/')
    :param local_folder_path: Path to the local folder to save to
    :return: True if the folder was downloaded, else False
    """
    if not folder_name.endswith('/'):
        folder_name += '/'

    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=folder_name)
        for obj in response.get('Contents', []):
            s3_object_name = obj['Key']
            local_file_path = os.path.join(
                local_folder_path, os.path.relpath(s3_object_name, folder_name))
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            s3_client.download_file(
                bucket_name, s3_object_name, local_file_path)
        return True
    except ClientError as e:
        logging.error(e)
        return False


def copy_file(source_bucket_name, source_object_name, dest_bucket_name, dest_object_name=None):
    """Copy a file from one S3 bucket to another

    :param source_bucket_name: Name of the source bucket
    :param source_object_name: Name of the source object
    :param dest_bucket_name: Name of the destination bucket
    :param dest_object_name: Name of the destination object. If not specified, source_object_name is used
    :return: True if the file was copied, else False
    """
    if dest_object_name is None:
        dest_object_name = source_object_name

    copy_source = {
        'Bucket': source_bucket_name,
        'Key': source_object_name
    }

    try:
        s3_client.copy(copy_source, dest_bucket_name, dest_object_name)
        return True
    except ClientError as e:
        logging.error(e)
        return False


def move_file(source_bucket_name, source_object_name, dest_bucket_name, dest_object_name=None):
    """Move a file from one S3 bucket to another (copy then delete)

    :param source_bucket_name: Name of the source bucket
    :param source_object_name: Name of the source object
    :param dest_bucket_name: Name of the destination bucket
    :param dest_object_name: Name of the destination object. If not specified, source_object_name is used
    :return: True if the file was moved, else False
    """
    if copy_file(source_bucket_name, source_object_name, dest_bucket_name, dest_object_name):
        return delete_file(source_bucket_name, source_object_name)
    return False


def get_file_url(bucket_name, object_name, region_name):
    """Get the public URL of a file in a bucket

    :param bucket_name: Name of the bucket
    :param object_name: Name of the object
    :param region_name: AWS region where the bucket is located
    :return: Public URL of the object if it exists, else None
    """
    try:
        url = f"https://{bucket_name}.s3.{region_name}.amazonaws.com/{object_name}"
        return url
    except ClientError as e:
        logging.error(e)
        return None


def empty_bucket(bucket_name):
    """Empty all files from a bucket

    :param bucket_name: Name of the bucket to empty
    :return: True if the bucket was emptied, else False
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        for obj in response.get('Contents', []):
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        return True
    except ClientError as e:
        logging.error(e)
        return False


def read_csv_from_s3(bucket_name, object_name):
    """Read a CSV file from S3 into a pandas DataFrame

    :param bucket_name: Name of the bucket
    :param object_name: Name of the object (CSV file)
    :return: pandas DataFrame if the file exists and is valid, else None
    """
    import pandas as pd
    from io import StringIO

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        return df
    except ClientError as e:
        logging.error(e)
        return None


def get_file_metadata(bucket_name, object_name):
    """Get metadata of a file in a bucket

    :param bucket_name: Name of the bucket
    :param object_name: Name of the object
    :return: Metadata dictionary if the object exists, else None
    """
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_name)
        return response['Metadata']
    except ClientError as e:
        logging.error(e)
        return None


def get_file_size(bucket_name, object_name):
    """Get the size of a file in a bucket

    :param bucket_name: Name of the bucket
    :param object_name: Name of the object
    :return: Size of the object in bytes if it exists, else None
    """
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_name)
        return response['ContentLength']
    except ClientError as e:
        logging.error(e)
        return None


def main():
    bucket_name = 'my-test-bucket-12345'
    file_name = 'test_file.txt'
    object_name = 'test_file.txt'

    # Create a bucket
    if create_bucket(bucket_name):
        print(f'Bucket "{bucket_name}" created successfully.')

    # List buckets
    print('Buckets in account:', bucket_list())

    # Upload a file
    if upload_file(bucket_name, file_name, object_name):
        print(f'File "{file_name}" uploaded successfully as "{object_name}".')

    # List files in the bucket
    print(f'Files in bucket "{bucket_name}":', list_files(bucket_name))

    # Download the file
    if download_file(bucket_name, object_name, 'downloaded_' + file_name):
        print(
            f'File "{object_name}" downloaded successfully as "downloaded_{file_name}".')

    # Delete the file
    if delete_file(bucket_name, object_name):
        print(f'File "{object_name}" deleted successfully.')

    # Delete the bucket
    if delete_bucket(bucket_name):
        print(f'Bucket "{bucket_name}" deleted successfully.')


if __name__ == '__main__':
    main()


# buckets = bucket_list()
# print(buckets)
# print(list_files(buckets[0]))
