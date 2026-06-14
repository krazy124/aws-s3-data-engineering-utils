"""Reusable PySpark / AWS Glue transformation helpers."""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    current_timestamp,
    expr,
    input_file_name,
    lower,
    regexp_replace,
    sum as spark_sum,
    to_date,
    to_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.types import DoubleType, StringType

logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")


def log_step(message):
    print("\n" + "=" * 60)
    print(f"[MonsterForge] {message}")
    print("=" * 60)


def print_quality_report(report):
    print("\n" + "=" * 60)
    print("MONSTERFORGE DATA QUALITY REPORT")
    print("=" * 60)

    for metric, value in report.items():
        label = metric.replace("_", " ").title()
        print(f"{label:<32} {value}")

    print("=" * 60)


def column_exists(df: DataFrame, column_name: str):
    """Check whether a column exists before applying a transformation."""
    if column_name not in df.columns:
        logging.warning(f"Column not found: {column_name}")
        return False

    return True


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


def trim_string_columns(df: DataFrame):
    """Trim leading and trailing whitespace from all string columns."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))

    return df


def lowercase_string_columns(df: DataFrame):
    """Lowercase all string columns."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, lower(col(field.name)))

    return df


def uppercase_column(df: DataFrame, column_name: str):
    """Uppercase one selected column."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(column_name, upper(col(column_name)))


def rename_column(df: DataFrame, old_name: str, new_name: str):
    """Rename one selected column."""
    if not column_exists(df, old_name):
        return df

    return df.withColumnRenamed(old_name, new_name)


def select_columns(df: DataFrame, columns_to_keep: list):
    """Select only requested columns that exist."""
    existing_columns = [
        column_name
        for column_name in columns_to_keep
        if column_name in df.columns
    ]

    return df.select(*existing_columns)


def drop_columns(df: DataFrame, columns_to_drop: list):
    """Drop selected columns if they exist."""
    existing_columns = [
        column_name
        for column_name in columns_to_drop
        if column_name in df.columns
    ]

    return df.drop(*existing_columns)


def remove_currency_symbols(df: DataFrame, column_name: str):
    """Remove simple currency symbols."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), r"[$,]", ""),
    )


def parse_currency_to_double(df: DataFrame, column_name: str):
    """Parse messy currency strings into double values."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), r"[^0-9.-]", "").cast(DoubleType()),
    )


def convert_to_integer(df: DataFrame, column_name: str):
    """Convert selected column to integer using Spark 4 safe try_cast."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(column_name, expr(f"try_cast({column_name} as int)"))


def convert_to_double(df: DataFrame, column_name: str):
    """Convert selected column to double."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(column_name, col(column_name).cast(DoubleType()))


def convert_to_date(df: DataFrame, column_name: str, date_format: str = "yyyy-MM-dd"):
    """Parse a selected column into date using one format."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(column_name, to_date(col(column_name), date_format))


def convert_to_timestamp(df: DataFrame, column_name: str, timestamp_format: str = "yyyy-MM-dd HH:mm:ss", ):
    """Parse a selected column into timestamp using one format."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(
        column_name,
        to_timestamp(col(column_name), timestamp_format),
    )


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


def fill_missing_values(df: DataFrame, fill_value="Unknown"):
    """Fill all missing values with one value."""
    return df.fillna(fill_value)


def fill_missing_column(df: DataFrame, column_name: str, fill_value):
    """Fill missing values in one selected column."""
    if not column_exists(df, column_name):
        return df

    return df.fillna({column_name: fill_value})


def remove_duplicates(df: DataFrame):
    """Remove fully duplicate rows."""
    return df.dropDuplicates()


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


def fix_negative_values_with_flag(df: DataFrame, column_name: str, flag_column_name: str = None, ):
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


def parse_multiple_date_formats(df: DataFrame, column_name: str,
                                output_column_name: str = None, date_formats: list = None, ):
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


def standardize_category_column(df: DataFrame, column_name: str, mapping: dict = None, ):
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


def standard_cleaning_pipeline(df: DataFrame):
    """Generic starter cleaning pipeline."""
    df = clean_column_names(df)
    df = trim_string_columns(df)
    df = remove_duplicates(df)

    return df


def validate_required_columns(df: DataFrame, required_columns: list):
    """Validate that all required columns exist in a DataFrame.

    Returns a small report dictionary so callers can decide whether to stop
    the pipeline or log a warning.
    """
    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in df.columns
    ]

    return {
        "is_valid": len(missing_columns) == 0,
        "required_columns": required_columns,
        "missing_columns": missing_columns,
    }


def normalize_empty_strings_to_null(df: DataFrame):
    """Convert blank string values to null across all string columns."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                when(trim(col(field.name)) == "", None).otherwise(col(field.name)),
            )

    return df


def add_ingestion_timestamp(df: DataFrame, column_name: str = "ingestion_timestamp"):
    """Add the current Spark processing timestamp to every row."""
    return df.withColumn(column_name, current_timestamp())


def add_source_file_column(df: DataFrame, column_name: str = "source_file"):
    """Add the source file path when Spark can identify it."""
    return df.withColumn(column_name, input_file_name())


def generate_null_count_report(df: DataFrame):
    """Return null counts for each column as a dictionary."""
    if not df.columns:
        return {}

    result_row = df.select(
        *[
            spark_sum(
                when(col(column_name).isNull(), 1).otherwise(0)
            ).alias(column_name)
            for column_name in df.columns
        ]
    ).collect()[0]

    return result_row.asDict()


def generate_duplicate_report(df: DataFrame, subset_columns: list = None):
    """Return duplicate metrics for the full row or a selected key subset."""
    total_rows = df.count()

    if subset_columns:
        existing_columns = [
            column_name
            for column_name in subset_columns
            if column_name in df.columns
        ]
    else:
        existing_columns = df.columns

    if not existing_columns:
        return {
            "total_rows": total_rows,
            "distinct_rows": total_rows,
            "duplicate_rows": 0,
            "subset_columns": [],
        }

    distinct_rows = df.dropDuplicates(existing_columns).count()

    return {
        "total_rows": total_rows,
        "distinct_rows": distinct_rows,
        "duplicate_rows": total_rows - distinct_rows,
        "subset_columns": existing_columns,
    }
