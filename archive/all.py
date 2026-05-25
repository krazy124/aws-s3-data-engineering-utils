# =========================================================
# LIST COMPREHENSIONS
# =========================================================

from importlib.resources import files
import os

import pandas as pd

# 1 Extracts bucket names from an existing S3 response and returns them as a python list
[bucket["Name"] for bucket in response["Buckets"]]

"""
response = s3_client.list_buckets()
bucket_names = []
for bucket in response["Buckets"]:
    bucket_names.append(bucket["Name"])
print(bucket_names)

"""


# 2 Calls the S3 client directly, extracts bucket names, and returns them as a python list
[bucket["Name"] for bucket in get_s3_client(region).list_buckets()["Buckets"]]

"""
bucket_names = []
s3_client = boto3.client("s3")


for buckets in s3_client:

"""

# 3 Returns a python list of object keys from the S3 client response
[obj["Key"] for obj in response["Contents"]]

# 4 Returns a dictionary of object keys and their metadata from the S3 client response
{obj["Key"]: obj for obj in response.get("Contents", [])}

# 5 Returns a python list of folder names from the S3 client response
[folder for folder in list_all_folders(
    selected_bucket) if folder != current_folder]

# 6 Returns only root-level file names that do not contain folder paths from the S3 client response
[file for file in all_files if "/" not in file]

# 7 Returns a dictionary mapping file paths to themselves from the S3 client response
{file: file for file in all_files}

# 8 Returns a python list of file names from the S3 client response that start with a specific location
[file for file in all_files if file.startswith(
    selected_location) and file != selected_location]

# 9 Returns a dictionary mapping shortened display file names to full S3 paths
{file.replace(selected_location, "", 1): file for file in files}

# 10 Returns a python list of full paths for the selected display files
[display_to_full_path[file]for file in selected_display_files]















# =========================================================
# PANDAS STATEMENTS
# =========================================================


df = pd.read_csv(StringIO(csv_content))

df = pd.DataFrame(bucket_names, columns=["Bucket Name"])

df = pd.DataFrame(response["Buckets"])

df = pd.DataFrame(files, columns=["File Name"])

df = pd.DataFrame(response["Contents"])

df["Size_MB"] = df["Size"] / 1024 / 1024

large_files = df[df["Size_MB"] > 100]


# =========================================================
# BOTO3 RESPONSE PATTERNS
# =========================================================

response = s3_client.list_buckets()

response = s3_client.list_objects_v2(Bucket=bucket_name)

response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

response = s3_client.get_object(Bucket=bucket_name, Key=object_name)

response = s3_client.head_object(Bucket=bucket_name, Key=object_name)


# =========================================================
# STRING / PATH PROCESSING
# =========================================================

relative_path = os.path.relpath(local_file_path, local_folder_path)

s3_object_name = os.path.join(folder_name, relative_path).replace("\\", "/")

relative_path = os.path.relpath(s3_object_name, folder_name)

local_file_path = os.path.join(local_folder_path, relative_path)

folder_name = source_folder.rstrip("/").split("/")[-1]

file_name = os.path.basename(file)

# =========================================================
# REALISTIC DATA ENGINEERING FLOW EXAMPLES
# =========================================================

response = s3_client.list_objects_v2(Bucket=bucket_name)

df = pd.DataFrame(response["Contents"])

df["Size_MB"] = df["Size"] / 1024 / 1024

large_files = df[df["Size_MB"] > 100]

csv_content = response["Body"].read().decode("utf-8")

df = pd.read_csv(StringIO(csv_content))
