from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# Create spark session with hive enabled
spark = SparkSession.builder.appName("carInsuranceClaimsApp").enableHiveSupport().getOrCreate()
# .config("spark.jars", "/Users/hmakhlouf/Desktop/TechCnsltng_WorkSpace/config/postgresql-42.7.2.jar") \

# 1- Establish the connection to PostgresSQL and hive:

# PostgresSQL connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver",
}
postgres_table_name = "car_insurance_claims"

# hive database and table names
hive_database_name = "project1db"
hive_table_name = "carinsuranceclaims"


# 2. read & show new dataset from PostgresSQL:
postgres_df = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
postgres_df.show(3)


#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
#-+-+--+-+--+-+--+-+--+-+-Transformations-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--
#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-

# Rename column from "ID" to "policy_number"
postgres_df = postgres_df.withColumnRenamed("ID", "POLICY_NUMBER")
postgres_df.show(3)
# Use Spark SQL to rename the column in Hive can not be made => remember hive table are made immutable and can not be updated
#spark.sql("USE {}".format(hive_database_name))
#spark.sql("ALTER TABLE {} REPLACE COLUMN ID POLICY_NUMBER INT".format(hive_table_name))


# Specify the column to be modified
columns_to_modify = ["MSTATUS", "GENDER", "EDUCATION", "OCCUPATION", "CAR_TYPE", "URBANICITY"]

# Modify string values by removing "z_"
for column in columns_to_modify:
    postgres_df = postgres_df.withColumn(column, regexp_replace(col(column), "^z_", ""))


#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-


# 3. read and show the existing_data in hive table
existing_hive_data = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))    #table("project1db.carinsuranceclaims")
existing_hive_data.show(3)

# 4. Determine the incremental data
#incremental_data_df = postgres_df.join(existing_hive_data.select("id"), postgres_df["id"] == existing_hive_data["id"], "left_anti")
incremental_data_df = postgres_df.join(existing_hive_data.select("POLICY_NUMBER"), postgres_df["POLICY_NUMBER"] == existing_hive_data["POLICY_NUMBER"], "left_anti")
print('------------------Incremental data-----------------------')
incremental_data_df.show()


# counting the number of the new records added to postgres tables
new_records = incremental_data_df.count()
print('------------------COUNTING INCREMENT RECORDS ------------')
print('new records added count', new_records)

# 5.  Adding the incremental_data DataFrame to the existing hive table
# Check if there are extra rows in PostgresSQL. if exist => # write & append to the Hive table
if incremental_data_df.count() > 0:
    # Append new rows to Hive table
    incremental_data_df.write.mode("append").insertInto("{}.{}".format(hive_database_name, hive_table_name))
    print("Appended {} new records to Hive table.".format(incremental_data_df.count()))
else:
    print("No new records been inserted in PostgresSQL table.")



# 7. Stop Spark session
spark.stop()



