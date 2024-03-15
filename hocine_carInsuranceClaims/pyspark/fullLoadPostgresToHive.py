# from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col

# Create spark session with hive enabled
spark = SparkSession.builder.appName("carInsuranceClaimsApp").enableHiveSupport().getOrCreate()
# .config("spark.jars", "/Users/hmakhlouf/Desktop/TechCnsltng_WorkSpace/config/postgresql-42.7.2.jar") \


## 1- Establish the connection to PostgresSQL and read data from the postgres Database -testdb
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
df_postgres.show(3)


#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
#-+-+--+-+--+-+--+-+--+-+-Transformations-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-

# Rename column from "ID" to "policy_number"
df_postgres = df_postgres.withColumnRenamed("ID", "POLICY_NUMBER")
df_postgres.printSchema()


# Specify the column to be modified
columns_to_modify = ["MSTATUS", "GENDER", "EDUCATION", "OCCUPATION", "CAR_TYPE", "URBANICITY"]
# Modify string values by removing "z_"
for column in columns_to_modify:
    df_postgres = df_postgres.withColumn(column, regexp_replace(col(column), "^z_", ""))

df_postgres.show(3)
#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-

## 2. load df_postgres to hive table
# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS project1db")

# Hive database and table names
hive_database_name = "project1db"
hive_table_name = "carInsuranceClaims"

# Create Hive Internal table over project1db
df_postgres.write.mode('overwrite').saveAsTable("{}.{}".format(hive_database_name, hive_table_name))

# Read Hive table
df = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))
df.show()

# Stop Spark session
spark.stop()
