import boto3
from pprint import pprint
import notes.bag_of_tricks as tricks


s3 = boto3.client("s3")
glue = boto3.client('glue')
