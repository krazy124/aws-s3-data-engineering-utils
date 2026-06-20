import boto3
from pprint import pprint


# Flattens dictionaries
def flatten_dict_paths(data, path=""):
    result = {}

    for key, value in data.items():
        current_path = f"{path}[{key}]"

        if isinstance(value, dict):
            result[current_path] = "<dict>"
            result.update(flatten_dict_paths(value, current_path))
        elif isinstance(value, list):
            result[current_path] = f"<list: {len(value)} items>"
        else:
            result[current_path] = value

    return result

# --------------------------------------------------------


def flatten_recursive_paths(data, path=""):
    result = {}

    if isinstance(data, dict):

        for key, value in data.items():
            current_path = f'{path}["{key}"]'

            if isinstance(value, (dict, list)):
                result.update(flatten_recursive_paths(value, current_path))
            else:
                result[current_path] = value

    elif isinstance(data, list):

        result[path] = f"<list: {len(data)} items>"

        for index, value in enumerate(data):
            current_path = f'{path}[{index}]'

            if isinstance(value, (dict, list)):
                result.update(flatten_recursive_paths(value, current_path))
            else:
                result[current_path] = value

    return result
# --------------------------------------------------------
# List Comprehension example
# bucket_names = [bucket["Name"] for bucket in bucket_list["Buckets"]]

# response = s3.list_objects_v2(Bucket=bucket_name)

# --------------------------------------------------------
# Shows the keys if a dictionary


def show_keys(obj, indent=0):
    if isinstance(obj, dict):
        for key, value in obj.items():
            print("    " * indent + str(key))
            show_keys(value, indent + 1)

    elif isinstance(obj, list) and len(obj) > 0:
        show_keys(obj[0], indent)


# show_keys(response)
# --------------------------------------------------------
if __name__ == "__main__":
    s3 = boto3.client("s3")
    athena = boto3.client("athena")
    glue = boto3.client("glue")

    s3 = boto3.client("s3")
    bucket_list = s3.list_buckets()
    bucket_name = "wlmdatawizard-monsterforge-873851887650"

    response = s3.list_objects_v2(Bucket=bucket_name)

    print(flatten_dict_paths(response))
