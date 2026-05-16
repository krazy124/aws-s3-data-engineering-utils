""" this file contains the main function to test the AWS S3 operations defined in aws_operations.py.
 Eventually this will be the entry point for the command line interface (CLI) of the project. """


def main():
    bucket_name = "my-test-bucket-12345"
    file_name = "test_file.txt"
    object_name = "test_file.txt"

    if create_bucket(bucket_name):
        print(f'Bucket "{bucket_name}" created successfully.')

    print("Buckets in account:", bucket_list())

    if upload_file(bucket_name, file_name, object_name):
        print(f'File "{file_name}" uploaded successfully as "{object_name}".')

    print(f'Files in bucket "{bucket_name}":', list_files(bucket_name))

    if download_file(bucket_name, object_name, "downloaded_" + file_name):
        print(f'File "{object_name}" downloaded successfully.')

    if delete_file(bucket_name, object_name):
        print(f'File "{object_name}" deleted successfully.')

    if delete_bucket(bucket_name):
        print(f'Bucket "{bucket_name}" deleted successfully.')
