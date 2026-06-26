for column_name, data_type in df.dtypes:
    if data_type == "string":
        df = df.withColumn(column_name, trim(col(column_name)))

for old_name in df.columns:
    new_name = old_name.lower().replace(" ", "_")
    df = df.withColumnRenamed(old_name, new_name)

df = df.withColumn("monster_type", lower(df["monster_type"]))
df = df.withColumn("status",       lower(df["status"]))
df = df.withColumn("base_price",   regexp_replace("base_price", r"[$,]|USD ", ""))
df = df.withColumn("base_price", col("base_price").cast("double"))
df = df.withColumn("price_was_negative", when(col("base_price") < 0, True).otherwise(False))
df = df.withColumn("base_price", regexp_replace("base_price", r"[-]", ""))
df = df.withColumn("original_date", col("created_date"))
df = df.withColumn("created_date", coalesce(expr("try_to_date(`created_date`, 'M/d/yy')"), expr("try_to_date(`created_date`, 'yyyy-MM-dd')"),
                   expr("try_to_date(`created_date`, 'yyyy/MM/dd')"), expr("try_to_date(`created_date`, 'MMM d yyyy')"), expr("try_to_date(`created_date`, 'MM-dd-yyyy')")))
df = df.withColumn("original_danger_level", col("danger_level"))
df = df.withColumn("danger_level", expr("try_cast(danger_level as int)"))
df = df.withColumn("invalid_danger_level", col("danger_level").isNull() | (col("danger_level") < 1) | (col("danger_level") > 10))
df = df.withColumn("monster_name_is_null", col("monster_name").isNull())
df = df.withColumn("date_is_null", col("created_date").isNull())
df = df.select("monster_id", "monster_name", "monster_type", "danger_level", "created_date", "base_price", "status", "original_date",
               "original_danger_level", "monster_name_is_null", "date_is_null", "invalid_danger_level", "price_was_negative")


"""{
    "transformation": "try_cast_int",
    "columns": ["danger_level"],
    "new_column": "danger_level",
    "preserve_original": True
}"""

"""groupBy (to group your data by a specific column and perform aggregate functions), 
   filter (to pick rows based on conditions), select (to choose specific columns), 
   orderBy (to sort your data), join (to combine two DataFrames) df.printSchema() df.show(5)"""

""" lower(), upper(), trim(), regexp_replace(), cast(), to_date(), when(), otherwise(), isNull(), dropDuplicates(), """

""" Create Clients ↓ s3 = boto3.client("s3") glue = boto3.client("glue") athena = boto3.client("athena") 
↓ Upload Raw File to S3 ↓ Run ETL Transformations ↓ Write Clean Data to S3 ↓ Write Quarantine Data to S3 
↓ Write Quality Report to S3 ↓ Start Glue Crawler ↓ Wait For Crawler To Finish ↓ Run Athena Query 
↓ Wait For Query To Finish ↓ Retrieve Results ↓ Display Results """


cat = ("function", "column_name")
df = df.withColumn(cat[1], cat[0](col(cat[1])))
