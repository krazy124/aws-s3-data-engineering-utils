import boto3
from pprint import pprint

s3 = boto3.client("s3")


bucket_name = "wlmdatawizard-monsterforge-873851887650"

response = s3.list_objects_v2(Bucket=bucket_name)


def show_keys(obj, indent=0):
    if isinstance(obj, dict):
        for key, value in obj.items():
            print("    " * indent + str(key))
            show_keys(value, indent + 1)

    elif isinstance(obj, list) and len(obj) > 0:
        show_keys(obj[0], indent)


show_keys(response)
