from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CompareSchema") \
    .enableHiveSupport() \
    .getOrCreate()

# Define PostgreSQL table and Hive table names
postgres_table_name = "your_postgres_table"
hive_table_name = "your_hive_table"

# Retrieve PostgreSQL table schema
postgres_schema = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://your_postgres_host:your_postgres_port/your_database") \
    .option("dbtable", postgres_table_name) \
    .option("user", "your_postgres_username") \
    .option("password", "your_postgres_password") \
    .load() \
    .schema

# Retrieve Hive table schema
hive_schema = spark.table(hive_table_name).schema

# Compare schemas
postgres_column_names = [field.name for field in postgres_schema.fields]
hive_column_names = [field.name for field in hive_schema.fields]

if set(postgres_column_names) == set(hive_column_names):
    print("Column names match between PostgreSQL and Hive tables.")
else:
    print("Column names do not match between PostgreSQL and Hive tables.")

# Check for data type mismatches
for postgres_field, hive_field in zip(postgres_schema.fields, hive_schema.fields):
    if postgres_field.name != hive_field.name or postgres_field.dataType != hive_field.dataType:
        print(f"Column '{postgres_field.name}' has a data type mismatch between PostgreSQL and Hive tables.")
        print(f"PostgreSQL data type: {postgres_field.dataType}")
        print(f"Hive data type: {hive_field.dataType}")

# Stop Spark session
spark.stop()
