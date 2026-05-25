# pylint: skip-file
import boto3
import pandas as pd


s3_client = boto3.client("s3")  # 1. CONNECT TO AWS

response = s3_client.list_objects_v2(Bucket="raw-data")  # 2. EXTRACT OBJECT METADATA DATA

files = [obj["Key"] for obj in response["Contents"]]  # 3. EXTRACT OBJECT PATHS FROM METADATA

s3_client.download_file("raw-data", "orders.csv","orders.csv")  # 4. DOWNLOAD DATA ONTO COMPUTER

df = pd.read_csv("orders.csv")  # 5. LOAD INTO PANDAS DATA FRAME

df.columns = df.columns.str.lower()  # 6. TRANSFORM / CLEAN DATA
df = df.drop_duplicates()

high_value_orders = df[df["amount"] > 1000]  # 7. FILTER / ANALYZE DATA

high_value_orders.to_csv("clean_orders.csv", index=False) # 8. SAVE TRANSFORMED DATA

s3_client.upload_file("clean_orders.csv", "processed-data","clean_orders.csv")  # 9. UPLOAD CLEAN DATA
                      
print("Pipeline completed successfully.")  # 10. LOG / VERIFY
