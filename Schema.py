from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CompareSchema") \
    .enableHiveSupport() \
    .getOrCreate()

# Define PostgreSQL table and Hive table names
postgres_table_name = "people"
hive_table_name = "usukprjdb.people"

# Retrieve PostgreSQL table schema
postgres_schema = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://your_postgres_host:your_postgres_port/your_database") \
    .option("dbtable", postgres_table_name) \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .load() \
    .schema

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CompareSchema") \
    .enableHiveSupport() \
    .getOrCreate()

# Define PostgreSQL table and Hive table names
postgres_table_name = "people"
hive_table_name = "usukprjdb.people"

# Retrieve PostgreSQL table schema
postgres_schema = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
    .option("dbtable", people) \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .load() \
    .schema

# Retrieve Hive table schema
hive_schema = spark.table(hive_table_name).schema

# Compare schemas
postgres_column_names = ["age"] 
from pyspark.sql.types import StructType, StructField, StringType

postgres_fields = [StructField(name, StringType(), True) for name in postgres_column_names]
postgres_schema = StructType(postgres_fields)

hive_column_names = [field.name for field in hive_schema.fields]

if set(postgres_column_names) == set(hive_column_names):
    print("Column names match between PostgreSQL and Hive tables.")
else:
    print("Column names do not match between PostgreSQL and Hive tables.")

# Check for data type mismatches
for postgres_field, hive_field in zip(postgres_schema.fields, hive_schema.fields):
    if postgres_field.name != hive_field.name or postgres_field.dataType != hive_field.dataType:
        print("Column '{}' has a data type mismatch between PostgreSQL and Hive tables.".format(postgres_field.name))
        print("PostgreSQL data type: {}".format(postgres_field.dataType))
        print("Hive data type: {}".format(hive_field.dataType))


# Stop Spark session
spark.stop()