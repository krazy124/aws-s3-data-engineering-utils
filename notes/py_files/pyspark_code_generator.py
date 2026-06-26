"""
pyspark_code_generator.py

Generates PySpark transformation code from a metadata-driven
pipeline plan.

Author: William
Project: MonsterForge
"""


def generate_pyspark_code(pipeline_plan):
    """
    Prints PySpark transformation code based on a pipeline plan.

    Parameters
    ----------
    pipeline_plan : list[tuple]

    Example:
        [
            ("trim", "all_string_columns"),
            ("upper", ["monster_id"]),
            ("lower", ["monster_type"])
        ]
    """

    # print("# ------------------------------------------------------------------")
    # print("# Generated PySpark Transformations")
    # print("# ------------------------------------------------------------------\n")

    for transform_name, columns in pipeline_plan:

        # print(f"# {transform_name}: {columns}")

        match transform_name.lower():

            case "normalize_headers":
                print("for old_name in df.columns:")
                print('    new_name = old_name.lower().replace(" ", "_")')
                print("    df = df.withColumnRenamed(old_name, new_name)")

            case "trim":
                print("for column_name, data_type in df.dtypes:")
                print('    if data_type == "string":')
                print("        df = df.withColumn(column_name, trim(col(column_name)))")

            case "blank_to_null":
                print("for column_name, data_type in df.dtypes:")
                print('    if data_type == "string":')
                print('        df = df.withColumn(column_name, when(trim(col(column_name)) == "", None).otherwise(col(column_name)))')

            case "upper":
                for column in columns:
                    print(f'df = df.withColumn("{column}", upper(col("{column}")))')

            case "lower":
                for column in columns:
                    print(f'df = df.withColumn("{column}", lower(col("{column}")))')

            case "initcap":
                for column in columns:
                    print(f'df = df.withColumn("{column}", initcap(col("{column}")))')

            case "preserve_original":
                for column in columns:
                    print(f'df = df.withColumn("preserve_{column}", col("{column}"))')

            case "clean_currency":
                for column in columns:
                    print(f'df = df.withColumn("{column}", regexp_replace("{column}", r"[$,]", ""))')
                    print(f'df = df.withColumn("{column}", regexp_replace("{column}", r"USD\\s*", ""))')

            case "try_cast_double":
                for column in columns:
                    print(f'df = df.withColumn("{column}", col("{column}").cast("double"))')

            case "flag_negative":
                for column in columns:
                    print(f'df = df.withColumn("original_{column}_negative", when(col("{column}") < 0, True).otherwise(False))')

            case "fix_negative":
                for column in columns:
                    print(f'df = df.withColumn("{column}", when(col("{column}") < 0, col("{column}") * -1).otherwise(col("{column}")))')

            case "try_parse_date":
                for column in columns:
                    print(
                        f'df = df.withColumn("{column}", '
                        f'coalesce('
                        f'expr("try_to_date(`{column}`, \'M/d/yy\')"), '
                        f'expr("try_to_date(`{column}`, \'yyyy-MM-dd\')"), '
                        f'expr("try_to_date(`{column}`, \'yyyy/MM/dd\')"), '
                        f'expr("try_to_date(`{column}`, \'MMM d yyyy\')"), '
                        f'expr("try_to_date(`{column}`, \'MM-dd-yyyy\')")))'
                    )

            case "try_cast_int":
                for column in columns:
                    print(f'df = df.withColumn("{column}", expr("try_cast({column} as int)"))')

            case "range_check":
                for column in columns:
                    print(f"# TODO: Add range validation for {column}")

            case "null_check":
                for column in columns:
                    print(f"# TODO: Add null validation for {column}")

            case _:
                print(f'# WARNING: "{transform_name}" has not been implemented.')


pipeline_plan = [
    ("normalize_headers", "all_columns"),
    ("trim", "all_string_columns"),
    ("blank_to_null", "all_string_columns"),

    ("upper", ["monster_id"]),
    ("initcap", ["monster_name"]),
    ("lower", ["monster_type", "status"]),

    ("preserve_original", ["created_date", "danger_level", "base_price"]),

    ("clean_currency", ["base_price"]),
    ("try_cast_double", ["base_price"]),

    ("flag_negative", ["base_price"]),
    ("fix_negative", ["base_price"]),

    ("try_parse_date", ["created_date"]),
    ("try_cast_int", ["danger_level"]),

    ("range_check", ["danger_level"]),
    ("null_check", ["monster_name", "created_date"]),
]

generate_pyspark_code(pipeline_plan)

print()
