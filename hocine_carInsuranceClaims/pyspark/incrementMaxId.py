from pyspark.sql import *
from pyspark.sql.functions import *



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

# 2- from the existing hive table
spark = SparkSession.builder.master("local").appName("MiniProj").enableHiveSupport().getOrCreate()
max_id = spark.sql("SELECT max(policy_number) FROM hocinedb.carinsuranceclaims")
m_id = max_id.collect()[0][0]


# 2. read & show new dataset from PostgresSQL:
more_data = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
more_data.show(3)

# 3- transformations

more_data = more_data.withColumnRenamed("ID", "POLICY_NUMBER")
# Specify the column to be modified
columns_to_modify = ["MSTATUS", "GENDER", "EDUCATION", "OCCUPATION", "CAR_TYPE", "URBANICITY"]
# Modify string values by removing "z_"
for column in columns_to_modify:
    more_data = more_data.withColumn(column, regexp_replace(col(column), "^z_", ""))

more_data.printSchema()


# Register the DataFrame as a temporary view
more_data.createOrReplaceTempView("my_car_insurance_claims")
# Run a SQL query on the DataFrame
query = 'SELECT * FROM my_car_insurance_claims WHERE POLICY_NUMBER > ' + str(m_id)
new_records = spark.sql(query)

print('------------------COUNTING INCREMENT RECORDS ------------')
print('new records added count', new_records)

more_data.write.mode("append").saveAsTable("hocinedb.carinsuranceclaims")


# Stop Spark session
spark.stop()
