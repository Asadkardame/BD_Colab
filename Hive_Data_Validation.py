from pyspark.sql import SparkSession
from pyspark.sql.functions import count, isnull, when

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Incremental Data Validation") \
    .enableHiveSupport() \
    .getOrCreate()
# Configure JDBC connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver",
}

# # Read new data from data source into a DataFrame (e.g., PostgreSQL)
# new_data_df = spark.read.jdbc(postgres_url, "select * from car_insurance_claims", properties=postgres_properties)

# Read new data from data source into a DataFrame (e.g., PostgreSQL)
new_data_df = spark.read.jdbc(url=postgres_url, table="(select * from car_insurance_claims) as tmp", properties=postgres_properties)

# Read existing data from Hive table into a DataFrame
existing_data_df = spark.table("project1db.carinsuranceclaims")


# Read existing data from Hive table into a DataFrame
existing_data_df = spark.table("project1db.carinsuranceclaims")

# Data Validation Checks
# 1. Check for missing values in new data
new_missing_values = new_data_df.select([count(when(isnull(c), c)).alias(c) for c in new_data_df.columns]).collect()[0]
print("Missing Values in New Data:", new_missing_values)

# 2. Check for missing values in existing data
existing_missing_values = existing_data_df.select([count(when(isnull(c), c)).alias(c) for c in existing_data_df.columns]).collect()[0]
print("Missing Values in Existing Data:", existing_missing_values)

# 3. Check for duplicate rows in new data
new_unique_rows = new_data_df.distinct().count()
new_total_rows = new_data_df.count()
if new_unique_rows != new_total_rows:
    print("New Data contains duplicate rows.")

# 4. Check for duplicate rows in existing data
existing_unique_rows = existing_data_df.distinct().count()
existing_total_rows = existing_data_df.count()
if existing_unique_rows != existing_total_rows:
    print("Existing Data contains duplicate rows.", existing_unique_rows)

# Additional validation checks can be added as needed

# Stop SparkSession
spark.stop()
