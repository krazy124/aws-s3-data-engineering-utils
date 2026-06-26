import boto3
from pprint import pprint
import notes.py_files.bag_of_tricks as tricks


s3 = boto3.client("s3")
glue = boto3.client("glue")
bucket_name = "wlmdatawizard-monsterforge-873851887650"

response = s3.list_objects_v2(Bucket=bucket_name)
crawler_meta = glue.list_crawlers()
# crawler_names = crawler_meta["CrawlerNames"]
flat_recurs = tricks.flatten_recursive_paths(response)
# test = [content["Key"] for content in response]
flat_dict = tricks.flatten_dict_paths(response)

list_keys = [tag["Key"] for tag in response['Contents']]
list_keys_dictionary = tricks.flatten_recursive_paths(list_keys)

pprint(list_keys_dictionary[""])

# pprint(flat_recurs)
