"""Example Glue/PySpark ETL job entry points."""

from pyspark.sql import SparkSession

from glue_transformations import standard_cleaning_pipeline


def load_schema_config(path):
    """Placeholder import shim: replace with your real pipeline_operations.load_schema_config."""
    from src.pipeline_operations import load_schema_config as _load_schema_config
    return _load_schema_config(path)


def apply_schema_metadata(df, schema_metadata):
    """Placeholder import shim: replace with your real apply_schema_metadata import."""
    from src.glue_transformations import apply_schema_metadata as _apply_schema_metadata
    return _apply_schema_metadata(df, schema_metadata)

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
