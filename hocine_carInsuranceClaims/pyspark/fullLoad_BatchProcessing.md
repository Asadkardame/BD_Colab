# Batch Data Processing Documentation: Full Load

## 1. Introduction:
Batch data processing involves the execution of a series of data processing tasks on a predefined dataset. In this documentation, we'll outline the process of performing a full load, where the entire dataset from a source (PostgreSQL) is processed and loaded into a target (Hive) database using PySpark. We'll also highlight the role of GitHub for code hosting and Jenkins for CI/CD orchestration.


## The full load process involves the following steps:

1. Importing necessary modules.
2. Creating a Spark session with Hive support.
3. Establishing connection to PostgreSQL and reading data.
4. Performing data transformations.
5. Creating Hive table and loading data.
6. Stopping Spark session.

## PySpark full load Code Overview: 
```
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

# read data from postgres table into dataframe :
df_postgres = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
df_postgres.show(3)


#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
## 2. -+-+--+-+--+-+--+-+--+-+-Transformations-+-+--+-+--+-+--+-+--+-+--+-+--+-+-
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

## 3. load df_postgres to hive table
# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS project1db")

# 3. Create Hive Internal table over project1db

# Hive database and table names
hive_database_name = "project1db"
hive_table_name = "carInsuranceClaims"

df_postgres.write.mode('overwrite').saveAsTable("{}.{}".format(hive_database_name, hive_table_name))

# 4. Read Hive table
df = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))
df.show()

# 5. Stop Spark session
spark.stop()

```

## 2. The provided PySpark code executes the following steps:

Step 1: Importing Necessary Modules

The code imports essential modules from the PySpark library required for data processing.
Step 2: Creating Spark Session with Hive Support

It creates a Spark session with Hive support enabled, facilitating interaction with Hive databases.
Step 3: Establishing Connection to PostgreSQL and Reading Data

Connection properties for PostgreSQL are defined.
Data is read from the PostgreSQL table into a DataFrame.
Step 4: Data Transformations

Renaming the column "ID" to "POLICY_NUMBER".
Modifying string values in specific columns by removing the prefix "z_" using regular expressions.
Step 5: Creating Hive Table and Loading Data

A Hive database is created if it doesn't exist.
The DataFrame is saved as a Hive table.
Step 6: Stopping Spark Session

The Spark session is stopped after completing the data processing tasks.
## 3. GitHub and Jenkins Integration:
link to fullLoadPostgresToHive.py file in GitHub repository : https://github.com/Asadkardame/BD_Colab/blob/hocine_branch/hocine_carInsuranceClaims/pyspark/fullLoadPostgresToHive.py 

link to Jenkins full load pyspark job: http://3.9.191.104:8080/view/Hocine/job/hocine_pysparkapp_job/


GitHub serves as the repository for hosting the PySpark code. It allows version control, collaboration, and code sharing.
Jenkins is utilized for Continuous Integration/Continuous Deployment (CI/CD) processes. It monitors GitHub repositories for changes and triggers automated builds and tests whenever new code is pushed.
Jenkins executes the PySpark job defined in the code whenever there's a new commit or code change in the GitHub repository.
## 4. Explanation:

The code reads data from a PostgreSQL database, performs necessary transformations, and loads it into a Hive table.
GitHub ensures version control and collaborative development of the PySpark code.
Jenkins automates the execution of the PySpark job as part of the CI/CD pipeline, ensuring seamless deployment and data processing.
## 5. Conclusion:
This documentation outlines the process of batch data processing for a full load using PySpark, highlighting the integration of GitHub and Jenkins for code hosting and CI/CD orchestration. The provided PySpark code efficiently transfers data from a PostgreSQL source to a Hive target, ensuring seamless data integration and processing.