# ==================================================
# glue_transformations.py
# Reusable PySpark / AWS Glue transformation helpers
# ==================================================

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
    regexp_replace,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import IntegerType, DoubleType, StringType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# ====== GT1.v1 - Clean Column Names ======

def clean_column_names(df: DataFrame) -> DataFrame:
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


# ====== GT2.v1 - Trim String Columns ======

def trim_string_columns(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))

    return df


# ====== GT3.v1 - Lowercase String Columns ======

def lowercase_string_columns(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, lower(col(field.name)))

    return df


# ====== GT4.v1 - Uppercase Selected Column ======

def uppercase_column(df: DataFrame, column_name: str) -> DataFrame:
    if column_name not in df.columns:
        logging.warning(f"Column not found: {column_name}")
        return df

    return df.withColumn(column_name, upper(col(column_name)))


# ====== GT5.v1 - Remove Duplicate Rows ======

def remove_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates()


# ====== GT6.v1 - Fill Missing Values ======

def fill_missing_values(df: DataFrame, fill_value="Unknown") -> DataFrame:
    return df.fillna(fill_value)


# ====== GT7.v1 - Fill Missing Values In Column ======

def fill_missing_column(df: DataFrame, column_name: str, fill_value) -> DataFrame:
    if column_name not in df.columns:
        logging.warning(f"Column not found: {column_name}")
        return df

    return df.fillna({column_name: fill_value})


# ====== GT8.v1 - Add Missing Flag Column ======

def add_missing_flag(df: DataFrame, column_name: str) -> DataFrame:
    if column_name not in df.columns:
        logging.warning(f"Column not found: {column_name}")
        return df

    flag_column = f"{column_name}_missing_flag"

    return df.withColumn(
        flag_column,
        when(col(column_name).isNull(), True).otherwise(False),
    )


# ====== GT9.v1 - Remove Currency Symbols ======

def remove_currency_symbols(df: DataFrame, column_name: str) -> DataFrame:
    if column_name not in df.columns:
        logging.warning(f"Column not found: {column_name}")
        return df

    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), r"[$,]", ""),
    )


# ====== GT10.v1 - Convert Column To Integer ======

def convert_to_integer(df: DataFrame, column_name: str) -> DataFrame:
    if column_name not in df.columns:
        logging.warning(f"Column not found: {column_name}")
        return df

    return df.withColumn(column_name, col(column_name).cast(IntegerType()))


# ====== GT11.v1 - Convert Column To Double ======

def convert_to_double(df: DataFrame, column_name: str) -> DataFrame:
    if column_name not in df.columns:
        logging.warning(f"Column not found: {column_name}")
        return df

    return df.withColumn(column_name, col(column_name).cast(DoubleType()))


# ====== GT12.v1 - Convert Column To Date ======

def convert_to_date(df: DataFrame, column_name: str, date_format="yyyy-MM-dd") -> DataFrame:
    if column_name not in df.columns:
        logging.warning(f"Column not found: {column_name}")
        return df

    return df.withColumn(column_name, to_date(col(column_name), date_format))


# ====== GT13.v1 - Convert Column To Timestamp ======

def convert_to_timestamp(
    df: DataFrame,
    column_name: str,
    timestamp_format="yyyy-MM-dd HH:mm:ss",
) -> DataFrame:
    if column_name not in df.columns:
        logging.warning(f"Column not found: {column_name}")
        return df

    return df.withColumn(
        column_name,
        to_timestamp(col(column_name), timestamp_format),
    )


# ====== GT14.v1 - Drop Columns ======

def drop_columns(df: DataFrame, columns_to_drop: list) -> DataFrame:
    existing_columns = [
        column_name
        for column_name in columns_to_drop
        if column_name in df.columns
    ]

    return df.drop(*existing_columns)


# ====== GT15.v1 - Rename Column ======

def rename_column(df: DataFrame, old_name: str, new_name: str) -> DataFrame:
    if old_name not in df.columns:
        logging.warning(f"Column not found: {old_name}")
        return df

    return df.withColumnRenamed(old_name, new_name)


# ====== GT16.v1 - Select Columns ======

def select_columns(df: DataFrame, columns_to_keep: list) -> DataFrame:
    existing_columns = [
        column_name
        for column_name in columns_to_keep
        if column_name in df.columns
    ]

    return df.select(*existing_columns)


# ====== GT17.v1 - Standard Cleaning Pipeline ======

def standard_cleaning_pipeline(df: DataFrame) -> DataFrame:
    df = clean_column_names(df)
    df = trim_string_columns(df)
    df = remove_duplicates(df)

    return df
