from pyspark.sql import SparkSession
from pyspark.sql.functions import count, isnull, when

try:
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Hive Data Validation") \
        .enableHiveSupport() \
        .getOrCreate()

    print("SparkSession initialized successfully.")

    # Hive table name
    hive_table_name = "usukprjdb.people"

    # Read data from Hive into a DataFrame
    df = spark.table(hive_table_name)

    print("Data loaded from Hive table.")

    # Data Validation Checks
    # Check for missing values
    missing_values = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).collect()[0]
    print("Missing Values:", missing_values)

    # Check for data types
    data_types = df.dtypes
    print("Data Types:", data_types)

    # Check for missing values in specific columns
    columns_to_check_null = ['occupation']
    missing_values_specific_columns = df.select([count(when(isnull(c), c)).alias(c) for c in columns_to_check_null]).collect()[0]
    print("Missing Values in Specific Columns:", missing_values_specific_columns)

    # Check for negative age values
    negative_age_count = df.filter(df["current_age"] < 0).count()
    if negative_age_count > 0:
        print("Data contains", negative_age_count, "rows with negative values in the 'current_age' column.")
    else:
        print("No negative values found in the 'current_age' column.")

    # Check for data types in specific columns
    columns_to_check_data_type = {'people_id': 'string'}
    incorrect_data_types = [(col_name, actual_type) for col_name, actual_type in df.dtypes if col_name.lower() in columns_to_check_data_type and actual_type != columns_to_check_data_type[col_name.lower()]]
    print("Incorrect Data Types in Specific Columns:", incorrect_data_types)

    # Check for unique values in specific columns
    columns_to_check_uniqueness = ['people_id']
    unique_values_specific_columns = {col_name: df.select(col_name).distinct().count() for col_name in columns_to_check_uniqueness}
    print("Unique Values in Specific Columns:", unique_values_specific_columns)

    # Check for uniqueness
    unique_rows = df.distinct().count()
    total_rows = df.count()
    if unique_rows != total_rows:
        print("Data contains duplicate rows.")
    else:
        print("Data does not contain duplicate rows.")

    # Check primary key constraint
    primary_key_check = df.select("people_id").distinct().count()
    if primary_key_check != total_rows:
        print("Primary key constraint violated for field people_id. Duplicate values found.")
    else:
        print("Primary key constraint satisfied for field people_id. All values are unique.")

except Exception as e:
    print("Error:", e)

finally:
    # Stop SparkSession
    spark.stop()
    print("SparkSession stopped.")
