# glue_transformations.py
# Reusable PySpark / AWS Glue transformation helpers
# MonsterForge Industries ETL Project

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    expr,
    lower,
    regexp_replace,
    to_date,
    to_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.types import DoubleType, StringType


logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# GT00.v1 - Log Pipeline Step
def log_step(message):
    print("\n" + "=" * 60)
    print(f"[MonsterForge] {message}")
    print("=" * 60)


# GT00.v2 - Print Quality Report
def print_quality_report(report):
    print("\n" + "=" * 60)
    print("MONSTERFORGE DATA QUALITY REPORT")
    print("=" * 60)

    for metric, value in report.items():
        label = metric.replace("_", " ").title()
        print(f"{label:<32} {value}")

    print("=" * 60)


# GT01.v1 - Column Exists
def column_exists(df: DataFrame, column_name: str):
    """Check whether a column exists before applying a transformation."""
    if column_name not in df.columns:
        logging.warning(f"Column not found: {column_name}")
        return False

    return True


# GT02.v1 - Clean Column Names
def clean_column_names(df: DataFrame):
    """Standardize column names to lowercase snake_case."""
    for old_name in df.columns:
        new_name = (
            old_name.strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("/", "_")
        )

        df = df.withColumnRenamed(old_name, new_name)

    return df


# GT03.v1 - Trim String Columns
def trim_string_columns(df: DataFrame):
    """Trim leading and trailing whitespace from all string columns."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))

    return df


# GT04.v1 - Lowercase String Columns
def lowercase_string_columns(df: DataFrame):
    """Lowercase all string columns."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, lower(col(field.name)))

    return df


# GT05.v1 - Uppercase Selected Column
def uppercase_column(df: DataFrame, column_name: str):
    """Uppercase one selected column."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(column_name, upper(col(column_name)))


# GT06.v1 - Rename Column
def rename_column(df: DataFrame, old_name: str, new_name: str):
    """Rename one selected column."""
    if not column_exists(df, old_name):
        return df

    return df.withColumnRenamed(old_name, new_name)


# GT07.v1 - Select Columns
def select_columns(df: DataFrame, columns_to_keep: list):
    """Select only requested columns that exist."""
    existing_columns = [
        column_name
        for column_name in columns_to_keep
        if column_name in df.columns
    ]

    return df.select(*existing_columns)


# GT08.v1 - Drop Columns
def drop_columns(df: DataFrame, columns_to_drop: list):
    """Drop selected columns if they exist."""
    existing_columns = [
        column_name
        for column_name in columns_to_drop
        if column_name in df.columns
    ]

    return df.drop(*existing_columns)


# GT09.v1 - Remove Currency Symbols
def remove_currency_symbols(df: DataFrame, column_name: str):
    """Remove simple currency symbols."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), r"[$,]", ""),
    )


# GT10.v1 - Parse Currency To Double
def parse_currency_to_double(df: DataFrame, column_name: str):
    """Parse messy currency strings into double values."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), r"[^0-9.-]", "").cast(DoubleType()),
    )


# GT11.v1 - Convert Column To Integer
def convert_to_integer(df: DataFrame, column_name: str):
    """Convert selected column to integer using Spark 4 safe try_cast."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(column_name, expr(f"try_cast({column_name} as int)"))


# GT12.v1 - Convert Column To Double
def convert_to_double(df: DataFrame, column_name: str):
    """Convert selected column to double."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(column_name, col(column_name).cast(DoubleType()))


# GT13.v1 - Convert Column To Date
def convert_to_date(df: DataFrame, column_name: str, date_format: str = "yyyy-MM-dd"):
    """Parse a selected column into date using one format."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(column_name, to_date(col(column_name), date_format))


# GT14.v1 - Convert Column To Timestamp
def convert_to_timestamp(
    df: DataFrame,
    column_name: str,
    timestamp_format: str = "yyyy-MM-dd HH:mm:ss",
):
    """Parse a selected column into timestamp using one format."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(
        column_name,
        to_timestamp(col(column_name), timestamp_format),
    )


# GT15.v1 - Add Missing Flag
def add_missing_flag(df: DataFrame, column_name: str):
    """Add a boolean flag showing whether a column value is missing."""
    if not column_exists(df, column_name):
        return df

    flag_column = f"{column_name}_missing_flag"

    return df.withColumn(
        flag_column,
        when(
            col(column_name).isNull() | (trim(col(column_name)) == ""),
            True,
        ).otherwise(False),
    )


# GT16.v1 - Fill Missing Values
def fill_missing_values(df: DataFrame, fill_value="Unknown"):
    """Fill all missing values with one value."""
    return df.fillna(fill_value)


# GT16.v2 - Fill Missing Values In Column
def fill_missing_column(df: DataFrame, column_name: str, fill_value):
    """Fill missing values in one selected column."""
    if not column_exists(df, column_name):
        return df

    return df.fillna({column_name: fill_value})


# GT17.v1 - Remove Duplicate Rows
def remove_duplicates(df: DataFrame):
    """Remove fully duplicate rows."""
    return df.dropDuplicates()


# GT17.v2 - Remove Duplicates By Columns
def remove_duplicates_by_columns(df: DataFrame, subset_columns: list):
    """Remove duplicates using selected key columns."""
    existing_columns = [
        column_name
        for column_name in subset_columns
        if column_name in df.columns
    ]

    if not existing_columns:
        logging.warning("No valid duplicate-check columns found.")
        return df

    return df.dropDuplicates(existing_columns)


# GT18.v1 - Fix Negative Values With Flag
def fix_negative_values_with_flag(
    df: DataFrame,
    column_name: str,
    flag_column_name: str = None,
):
    """Convert negative numeric values to positive values and add a correction flag."""
    if not column_exists(df, column_name):
        return df

    if flag_column_name is None:
        flag_column_name = f"{column_name}_corrected_flag"

    return (
        df.withColumn(
            flag_column_name,
            when(col(column_name) < 0, True).otherwise(False),
        )
        .withColumn(
            column_name,
            when(col(column_name) < 0, spark_abs(col(column_name))).otherwise(
                col(column_name)
            ),
        )
    )


# GT19.v1 - Parse Multiple Date Formats
def parse_multiple_date_formats(
    df: DataFrame,
    column_name: str,
    output_column_name: str = None,
    date_formats: list = None,
):
    """Parse several possible date formats using Spark 4 safe try_to_date."""
    if not column_exists(df, column_name):
        return df

    if output_column_name is None:
        output_column_name = column_name

    if date_formats is None:
        date_formats = [
            "yyyy-MM-dd",
            "M/d/yy",
            "MMM d yyyy",
            "MM-dd-yyyy",
            "yyyy/MM/dd",
        ]

    parsed_dates = [
        expr(f"try_to_date({column_name}, '{date_format}')")
        for date_format in date_formats
    ]

    return df.withColumn(output_column_name, coalesce(*parsed_dates))


# GT20.v1 - Standardize Category Column
def standardize_category_column(
    df: DataFrame,
    column_name: str,
    mapping: dict = None,
):
    """Standardize text category values."""
    if not column_exists(df, column_name):
        return df

    df = df.withColumn(column_name, lower(trim(col(column_name))))

    if mapping:
        cleaned_column = col(column_name)

        for raw_value, clean_value in mapping.items():
            cleaned_column = when(
                col(column_name) == raw_value.lower().strip(),
                clean_value.lower().strip(),
            ).otherwise(cleaned_column)

        df = df.withColumn(column_name, cleaned_column)

    return df


# GT21.v1 - Standard Cleaning Pipeline
def standard_cleaning_pipeline(df: DataFrame):
    """Generic starter cleaning pipeline."""
    df = clean_column_names(df)
    df = trim_string_columns(df)
    df = remove_duplicates(df)

    return df


if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("MonsterForge Transformation Test")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    input_path = "data/monster/MonsterForge_monsters_raw_100.csv"

    clean_output_path = "data/monster/output/clean/monsters"
    quarantine_output_path = "data/monster/output/quarantine/monsters"
    report_output_path = "data/monster/output/reports/quality_report"

    clean_df, quarantine_df, report = run_monsterforge_etl(
        spark=spark,
        input_path=input_path,
        clean_output_path=clean_output_path,
        quarantine_output_path=quarantine_output_path,
        report_output_path=report_output_path,
        preview=True,
    )

    log_step("MonsterForge Transformation Test Complete")
