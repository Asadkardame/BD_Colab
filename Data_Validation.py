from pyspark.sql import SparkSession
from pyspark.sql.functions import count, isnull, when

try:
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("PostgreSQL Data Validation") \
        .getOrCreate()

    print("SparkSession initialized successfully.")

    # Configure JDBC connection properties
    postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
    postgres_properties = {
        "user": "consultants",
        "password": "WelcomeItc@2022",
        "driver": "org.postgresql.Driver",
    }

    print("Connecting to PostgreSQL...")

    # Read data from PostgreSQL into a DataFrame
    df = spark.read.jdbc(postgres_url, "people2", properties=postgres_properties)

    print("Data loaded from PostgreSQL.")

    # Data Validation Checks
    # Check for missing values
    missing_values = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).collect()[0]
    print("Missing Values:", missing_values)

    # Check for data types
    data_types = df.dtypes
    print("Data Types:", data_types)

    # Data Validation Checks
    # Check for missing values in specific columns
    columns_to_check_null = ['PEOPLE_ID']
    missing_values_specific_columns = df.select([count(when(isnull(c), c)).alias(c) for c in columns_to_check_null]).collect()[0]
    print("Missing Values in Specific Columns:", missing_values_specific_columns)

    # Check for data types in specific columns
    columns_to_check_data_type = {'PEOPLE_ID': 'string'}
    incorrect_data_types = [(col_name, actual_type) for col_name, actual_type in df.dtypes if col_name in columns_to_check_data_type and actual_type != columns_to_check_data_type[col_name]]
    print("Incorrect Data Types in Specific Columns:", incorrect_data_types)

    # Check for unique values in specific columns
    columns_to_check_uniqueness = ['OCCUPATION']
    unique_values_specific_columns = {col_name: df.select(col_name).distinct().count() for col_name in columns_to_check_uniqueness}
    print("Unique Values in Specific Columns:", unique_values_specific_columns)

    # Check for uniqueness
    unique_rows = df.distinct().count()
    total_rows = df.count()
    if unique_rows != total_rows:
        print("Data contains duplicate rows.")
    else:
        print("Data does not contain duplicate rows.")

except Exception as e:
    print("Error:", e)

finally:
    # Stop SparkSession
    spark.stop()
    print("SparkSession stopped.")
