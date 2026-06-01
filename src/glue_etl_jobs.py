# glue_etl_jobs.py

from pyspark.sql import SparkSession

from src.glue_transformations import (
    standard_cleaning_pipeline,
    apply_schema_metadata,
)

from src.pipeline_operations import (
    load_schema_config,
)


def run_customer_etl():

    spark = SparkSession.builder.appName(
        "customer-etl"
    ).getOrCreate()

    raw_df = spark.read.csv(
        "data/dirty_data_25.csv",
        header=True,
        inferSchema=False,
    )

    clean_df = standard_cleaning_pipeline(raw_df)

    schema_metadata = load_schema_config(
        "configs/customer_schema.json"
    )

    typed_df = apply_schema_metadata(
        clean_df,
        schema_metadata,
    )

    typed_df.write.mode("overwrite").parquet(
        "output/customer_cleaned"
    )

    spark.stop()
