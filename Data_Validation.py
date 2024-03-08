from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("PostgreSQL Data Validation") \
    .getOrCreate()

# Configure JDBC connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver",
}

# Read data from PostgreSQL into a DataFrame
df = spark.read.jdbc(postgres_url, public.car_insurance_claims1, properties=postgres_properties)

# Data Validation Checks
# Check for missing values
missing_values = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).collect()[0]
print("Missing Values:", missing_values)

# Check for data types
data_types = df.dtypes
print("Data Types:", data_types)

# Check for uniqueness
unique_rows = df.distinct().count()
total_rows = df.count()
if unique_rows != total_rows:
    print("Data contains duplicate rows.")

# Additional validation checks can be added as needed

# Stop SparkSession
spark.stop()
