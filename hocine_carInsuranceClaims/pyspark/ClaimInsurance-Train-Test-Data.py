from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create spark session with hive enabled
spark = SparkSession.builder \
    .appName("carInsuranceClaimsApp") \
    .enableHiveSupport() \
    .getOrCreate()
# .config("spark.jars", "/Users/hmakhlouf/Desktop/TechCnsltng_WorkSpace/config/postgresql-42.7.2.jar") \


# PostgresSQL connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver",
}
postgres_table_name = "car_insurance_claims"

# Read data from PostgresSQL
df_postgres = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
df_postgres.show()


# Transformations
df_postgres = df_postgres.withColumn("id", col("id").cast("int"))

# Split data into train and test (60% train, 40% test)
train_df, test_df = df_postgres.randomSplit([0.6, 0.4], seed=42)

train_df.show(5)
test_df.show(5)

# naming tables
train_table_name = "train_carinsuranceclaims"
test_table_name = "test_carinsuranceclaims"

# Save train_df and test_df back to PostgresSQL
train_df.write.jdbc(url=postgres_url, table=train_table_name, mode="overwrite", properties=postgres_properties)
test_df.write.jdbc(url=postgres_url, table=test_table_name, mode="overwrite", properties=postgres_properties)


# load train and test df to hive project1db database
# Create Hive Internal table over project1db
hive_database_name = "project1db"
df_postgres.write.mode('overwrite').saveAsTable("{}.{}".format(hive_database_name, train_table_name))
df_postgres.write.mode('overwrite').saveAsTable("{}.{}".format(hive_database_name, test_table_name))

