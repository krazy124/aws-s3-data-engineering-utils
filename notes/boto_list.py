# ============================================================
# S3
# ============================================================

s3.create_bucket(Bucket="bucket-name")

s3.put_object(Bucket="bucket-name", Key="folder/file.txt", Body=data)

s3.list_buckets()

s3.list_objects_v2(Bucket="bucket-name", Prefix="folder/")

s3.delete_bucket(Bucket="bucket-name")

s3.delete_object(Bucket="bucket-name", Key="folder/file.txt")

s3.upload_file(Filename="local_file.csv", Bucket="bucket-name", Key="folder/file.csv")

s3.download_file(Bucket="bucket-name", Key="folder/file.csv", Filename="local_file.csv")

s3.copy(CopySource={"Bucket": "source-bucket", "Key": "file.csv"}, Bucket="dest-bucket", Key="file.csv")

s3.get_object(Bucket="bucket-name", Key="file.csv")


# ============================================================
# GLUE
# ============================================================

glue.get_databases()

glue.get_tables(DatabaseName="database_name")

glue.get_table(DatabaseName="database_name", Name="table_name")

glue.list_crawlers()

glue.create_crawler(Name="crawler-name", Role="GlueRole", DatabaseName="database_name", Targets={"S3Targets": [{"Path": "s3://bucket/folder/"}]})

glue.start_crawler(Name="crawler-name")

glue.get_crawler(Name="crawler-name")

glue.create_database(DatabaseInput={"Name": "database_name"})

glue.list_jobs()

glue.get_job(JobName="job-name")

glue.start_job_run(JobName="job-name")

glue.get_job_run(JobName="job-name", RunId="run-id")

glue.batch_stop_job_run(JobName="job-name", JobRunIds=["run-id"])


# ============================================================
# ATHENA
# ============================================================

athena.start_query_execution(QueryString="SELECT * FROM table", QueryExecutionContext={"Database": "database_name"}, ResultConfiguration={"OutputLocation": "s3://bucket/results/"})

athena.get_query_execution(QueryExecutionId="query-id")

athena.get_paginator("get_query_results")


# ============================================================
# IAM
# ============================================================

iam.get_paginator("list_roles")


# ============================================================
# CLOUDWATCH LOGS
# ============================================================

logs.get_log_events(logGroupName="group-name", logStreamName="stream-name")


# ============================================================
# CONNECTION TESTS
# ============================================================

s3.list_buckets()

glue.get_databases()

athena.list_work_groups()
