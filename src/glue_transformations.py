
# glue_transformations.py
# Reusable PySpark / AWS Glue transformation helpers
# MonsterForge Industries ETL Project


import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (abs as spark_abs, coalesce, col, lower, regexp_replace, to_date, to_timestamp, trim, upper, when, )
from pyspark.sql.types import DoubleType, IntegerType, StringType

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", )


# Validation Helper
def column_exists(df: DataFrame, column_name: str):
    """Check whether a column exists before applying a transformation."""
    if column_name not in df.columns:
        logging.warning(f"Column not found: {column_name}")
        return False

    return True


# GT1.v1 - Clean Column Names
def clean_column_names(df: DataFrame):
    """
    Standardize column names to lowercase snake_case.

    Example:
    Monster ID -> monster_id
    Base Price -> base_price
    """
    for old_name in df.columns:
        new_name = (old_name.strip() .lower() .replace(" ", "_") .replace("-", "_") .replace("/", "_"))

        df = df.withColumnRenamed(old_name, new_name)

    return df


# GT2.v1 - Trim String Columns
def trim_string_columns(df: DataFrame):
    """Trim leading and trailing whitespace from all string columns."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))

    return df


# GT3.v1 - Lowercase String Columns
def lowercase_string_columns(df: DataFrame):
    """Lowercase all string columns."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, lower(col(field.name)))

    return df


# GT4.v1 - Uppercase Selected Column
def uppercase_column(df: DataFrame, column_name: str):
    """Uppercase one selected column."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(column_name, upper(col(column_name)))


# GT5.v1 - Remove Duplicate Rows
def remove_duplicates(df: DataFrame):
    """Remove fully duplicate rows."""
    return df.dropDuplicates()


# GT5.v2 - Remove Duplicates By Columns
def remove_duplicates_by_columns(df: DataFrame, subset_columns: list):
    """
    Remove duplicates using selected key columns.

    Example:
    remove_duplicates_by_columns(df, ["monster_id"])
    """
    existing_columns = [
        column_name
        for column_name in subset_columns
        if column_name in df.columns
    ]

    if not existing_columns:
        logging.warning("No valid duplicate-check columns found.")
        return df

    return df.dropDuplicates(existing_columns)


# GT6.v1 - Fill Missing Values
def fill_missing_values(df: DataFrame, fill_value="Unknown"):
    """
    Fill all missing values with one value.

    Warning:
    This is useful for quick exploration, but avoid using it globally
    in production pipelines because numeric/date columns may need
    different handling than string columns.
    """
    return df.fillna(fill_value)


# GT7.v1 - Fill Missing Values In Column
def fill_missing_column(df: DataFrame, column_name: str, fill_value):
    """Fill missing values in one selected column."""
    if not column_exists(df, column_name):
        return df

    return df.fillna({column_name: fill_value})


# GT8.v1 - Add Missing Flag Column
def add_missing_flag(df: DataFrame, column_name: str):
    """
    Add a boolean flag showing whether a column value is missing.

    Example:
    monster_name_missing_flag
    """
    if not column_exists(df, column_name):
        return df

    flag_column = f"{column_name}_missing_flag"

    return df.withColumn(
        flag_column,
        when(col(column_name).isNull() | (trim(col(column_name)) == ""), True)
        .otherwise(False),
    )


# GT9.v1 - Remove Currency Symbols
def remove_currency_symbols(df: DataFrame, column_name: str):
    """
    Remove simple currency symbols.

    Example:
    $1,250.00 -> 1250.00

    For stronger parsing, use parse_currency_to_double().
    """
    if not column_exists(df, column_name):
        return df

    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), r"[$,]", ""),
    )


# GT10.v1 - Convert Column To Integer
def convert_to_integer(df: DataFrame, column_name: str):
    """Cast a selected column to integer."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(column_name, col(column_name).cast(IntegerType()))


# GT11.v1 - Convert Column To Double
def convert_to_double(df: DataFrame, column_name: str):
    """Cast a selected column to double."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(column_name, col(column_name).cast(DoubleType()))


# GT12.v1 - Convert Column To Date
def convert_to_date(df: DataFrame, column_name: str, date_format: str = "yyyy-MM-dd", ):
    """Parse a selected column into date using one date format."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(column_name, to_date(col(column_name), date_format))


# GT13.v1 - Convert Column To Timestamp
def convert_to_timestamp(df: DataFrame, column_name: str,
                         timestamp_format: str = "yyyy-MM-dd HH:mm:ss", ):
    """Parse a selected column into timestamp using one timestamp format."""
    if not column_exists(df, column_name):
        return df

    return df.withColumn(
        column_name,
        to_timestamp(col(column_name), timestamp_format),
    )


# GT14.v1 - Drop Columns
def drop_columns(df: DataFrame, columns_to_drop: list):
    """Drop selected columns if they exist."""
    existing_columns = [
        column_name
        for column_name in columns_to_drop
        if column_name in df.columns
    ]

    return df.drop(*existing_columns)


# GT15.v1 - Rename Column
def rename_column(df: DataFrame, old_name: str, new_name: str):
    """Rename one selected column."""
    if not column_exists(df, old_name):
        return df

    return df.withColumnRenamed(old_name, new_name)


# GT16.v1 - Select Columns
def select_columns(df: DataFrame, columns_to_keep: list):
    """Select only the requested columns that exist in the DataFrame."""
    existing_columns = [
        column_name
        for column_name in columns_to_keep
        if column_name in df.columns
    ]

    return df.select(*existing_columns)


# GT17.v1 - Standard Cleaning Pipeline
def standard_cleaning_pipeline(df: DataFrame):
    """
    Generic starter cleaning pipeline.

    This is intentionally simple and reusable.
    Dataset-specific pipelines should be built separately.
    """
    df = clean_column_names(df)
    df = trim_string_columns(df)
    df = remove_duplicates(df)

    return df


# GT18.v1 - Parse Currency To Double
def parse_currency_to_double(df: DataFrame, column_name: str):
    """
    Parse messy currency strings into double values.

    Handles examples like:
    $1,250.00
    1250 USD
    USD 1100
    3100.50
    -500
    """
    if not column_exists(df, column_name):
        return df

    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), r"[^0-9.-]", "").cast(DoubleType()),
    )


# GT19.v1 - Fix Negative Values With Flag
def fix_negative_values_with_flag(df: DataFrame, column_name: str, flag_column_name: str = None, ):
    """
    Convert negative numeric values to positive values and add a correction flag.

    Example:
    base_price = -500
    becomes:
    base_price = 500
    base_price_corrected_flag = True
    """
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
            when(col(column_name) < 0, spark_abs(col(column_name)))
            .otherwise(col(column_name)),
        )
    )


# GT20.v1 - Parse Multiple Date Formats
def parse_multiple_date_formats(df: DataFrame, column_name: str, output_column_name: str = None,
                                date_formats: list = None, ):
    """
    Parse a date column using multiple accepted date formats.

    MonsterForge date rule:
    MM-dd-yyyy means month-day-year.
    Example:
    05-01-2026 = May 1, 2026
    """
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
        to_date(col(column_name), date_format)
        for date_format in date_formats
    ]

    return df.withColumn(output_column_name, coalesce(*parsed_dates))


# GT21.v1 - Standardize Categories
def standardize_category_column(df: DataFrame, column_name: str, mapping: dict = None, ):
    """
    Standardize text category values.

    If no mapping is provided, the function trims and lowercases the column.
    If a mapping is provided, known values are replaced.

    Example mapping:
    {
        "undead": "zombie",
        "zombie": "zombie",
        "vampire": "vampire"
    }
    """
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


# MonsterForge Pipeline - monsters.csv
def clean_monsters(df: DataFrame):
    """
    MonsterForge-specific cleaning pipeline for monsters.csv.

    This pipeline demonstrates:
    1. Column renaming
    2. Type conversion
    3. Date parsing
    4. Currency parsing
    5. Null flagging
    6. Duplicate removal
    7. Category standardization
    """

    monster_type_mapping = {
        "zombie": "zombie",
        "undead": "zombie",
        "vampire": "vampire",
        "dragon": "dragon",
        "beast": "beast",
        "ice beast": "beast",
        "ghost": "ghost",
    }

    status_mapping = {
        "active": "active",
        "inactive": "inactive",
        "testing": "testing",
        "unknown": "unknown",
        "retired": "retired",
    }

    df = clean_column_names(df)
    df = trim_string_columns(df)

    # Add flags before conversions overwrite invalid values with nulls.
    df = add_missing_flag(df, "monster_name")
    df = add_missing_flag(df, "created_date")

    df = standardize_category_column(df, "monster_type", monster_type_mapping)
    df = standardize_category_column(df, "status", status_mapping)

    df = parse_currency_to_double(df, "base_price")
    df = fix_negative_values_with_flag(df, "base_price")

    df = parse_multiple_date_formats(df, "created_date")
    df = convert_to_integer(df, "danger_level")

    # Remove duplicate monster IDs while keeping the first record encountered.
    df = remove_duplicates_by_columns(df, ["monster_id"])

    expected_columns = ["monster_id", "monster_name", "monster_type",
                        "danger_level", "created_date", "base_price", "status",
                        "monster_name_missing_flag", "created_date_missing_flag",
                        "base_price_corrected_flag",]

    df = select_columns(df, expected_columns)

    return df
