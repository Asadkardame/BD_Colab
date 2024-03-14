from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CompareSchema") \
    .enableHiveSupport() \
    .getOrCreate()

# Define PostgreSQL table and Hive table names
postgres_table_name = "people"
hive_table_name = "usukprjdb.people"

# # Retrieve PostgreSQL table schema
# postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
# postgres_properties = {
#     "user": "consultants",
#     "password": "WelcomeItc@2022",
#     "driver": "org.postgresql.Driver",
# }

postgres_df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
    .option("dbtable", "people") \
    .option("user", "consultants") \
    .option("password", "WelcomeItc@2022") \
    .option("driver", "org.postgresql.Driver") \
    .load() 

# Extract schema from the DataFrame
postgres_schema = postgres_df.schema

# Retrieve Hive table schema
hive_schema = spark.table(hive_table_name).schema


# Compare schemas
postgres_column_names = [field.name for field in postgres_schema.fields]
hive_column_names = [field.name for field in hive_schema.fields]

if set(postgres_column_names) == set(hive_column_names):
    print("Column names match between PostgreSQL and Hive tables.")
    # Check for data type mismatches
    for postgres_field, hive_field in zip(postgres_schema.fields, hive_schema.fields):
        if postgres_field.name != hive_field.name or postgres_field.dataType != hive_field.dataType:
            print("Column '{}' has a data type mismatch between PostgreSQL and Hive tables.".format(postgres_field.name))
            print("PostgreSQL data type: {}".format(postgres_field.dataType))
            print("Hive data type: {}".format(hive_field.dataType))
        else:
            print("Column '{}' has a data type matches between PostgreSQL and Hive tables.".format(postgres_field.name))
            print("PostgreSQL data type: {}".format(postgres_field.dataType))
            print("Hive data type: {}".format(hive_field.dataType))
else:
    print("Column names do not match between PostgreSQL and Hive tables.")

# Stop Spark session
spark.stop()
