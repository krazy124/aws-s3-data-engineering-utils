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


def lambda_handler(event, context):
    main()
    return {
        'statusCode': 200,
        'body': 'AWS operations completed successfully.'
    }


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


def get_secret():
    secret_name = "MySecretName"
    region_name = "us-west-2"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print("The requested secret " + secret_name + " was not found")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print("The request was invalid due to:", e)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print("The request had invalid params:", e)
        elif e.response['Error']['Code'] == 'DecryptionFailure':
            print(
                "The requested secret can't be decrypted using the provided KMS key:", e)
        elif e.response['Error']['Code'] == 'InternalServiceError':
            print("An error occurred on service side:", e)
    else:
        # Secrets Manager decrypts the secret value using the associated KMS CMK
        # Depending on whether the secret was a string or binary, only one of these fields will be populated
        if 'SecretString' in get_secret_value_response:
            text_secret_data = get_secret_value_response['SecretString']
        else:
            binary_secret_data = get_secret_value_response['SecretBinary']

        # Your code goes here.
