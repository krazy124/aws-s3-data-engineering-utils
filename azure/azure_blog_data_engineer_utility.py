import os
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

print("Connected!")

def 