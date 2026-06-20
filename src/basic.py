import boto3
from pprint import pprint

s3 = boto3.client("s3")
athena = boto3.client("athena")
glue = boto3.client("glue")


# filename = "C:\Users\willi\OneDrive\Desktop\aws project\aws-s3-data-engineering-utils\data\monster\MonsterForge_monsters_raw_100.csv"
bucket = "wlmdatawizard-monsterforge-873851887650"

response = s3.list_objects(Bucket=bucket)


def show_keys(obj, indent=0):
    if isinstance(obj, dict):
        for key, value in obj.items():
            print("    " * indent + str(key))
            show_keys(value, indent + 1)

    elif isinstance(obj, list) and len(obj) > 0:
        show_keys(obj[0], indent)


# show_keys(response)

crawlers = glue.list_crawlers()
crawlers_names_list = crawlers["CrawlerNames"]
crawlers_meta = glue.get_crawlers()
pprint(crawlers_meta)


# show_keys(crawlers)
# print(crawlers["CrawlerNames"])

"""
Our S3 scheam:
raw/
  monsters/
    latest/
      monsters.csv
    runs/
      run_id=YYYYMMDD_HHMMSS/
        monsters.csv

clean/
  monsters/
    latest/
      monsters_clean.csv
    table/
      monsters_clean.csv
    runs/
      run_id=YYYYMMDD_HHMMSS/
        monsters_clean.csv

quarantine/
  monsters/
    latest/
      monsters_quarantine.csv
    table/
      monsters_quarantine.csv
    runs/
      run_id=YYYYMMDD_HHMMSS/
        monsters_quarantine.csv

reports/
  monsters/
    latest/
      quality_report.json
    runs/
      run_id=YYYYMMDD_HHMMSS/
        quality_report.json

athena-results/
"""
