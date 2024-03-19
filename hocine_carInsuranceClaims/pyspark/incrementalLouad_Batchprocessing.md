
## 1. Incremental Load Process
The incremental load process involves determining and processing only the new data added since the last load. This process includes:

1. Importing necessary modules.
2. Creating a Spark session with Hive support.
3. Establishing connection to PostgreSQL and reading data.
4. Performing data transformations.
5. Reading existing data from the Hive table.
6. Determining the incremental data.
7. Counting the number of new records added to PostgreSQL.
8. Adding the incremental data to the existing Hive table.
9. Stopping Spark session.


## 2. pyspark incremental load code Overview:
```
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


# 2. read data from postgres table into dataframe :
postgres_df = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
postgres_df.show(3)


#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
# 3. -+-+--+-+--+-+--+-+--+-+-Transformations-+-+--+-+--+-+--+-+--+-+--+-+--+-+--
#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-

# Rename column from "ID" to "policy_number"
postgres_df = postgres_df.withColumnRenamed("ID", "POLICY_NUMBER")
postgres_df.show(3)


# Specify the column to be modified
columns_to_modify = ["MSTATUS", "GENDER", "EDUCATION", "OCCUPATION", "CAR_TYPE", "URBANICITY"]

# Modify string values by removing "z_"
for column in columns_to_modify:
    postgres_df = postgres_df.withColumn(column, regexp_replace(col(column), "^z_", ""))

postgres_df.printSchema()
#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
#-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-


# 4. read and show the existing_data in hive table
existing_hive_data = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))    #table("project1db.carinsuranceclaims")
existing_hive_data.show(3)

# 5. Determine the incremental data
#incremental_data_df = postgres_df.join(existing_hive_data.select("id"), postgres_df["id"] == existing_hive_data["id"], "left_anti")
incremental_data_df = postgres_df.join(existing_hive_data.select("POLICY_NUMBER"), postgres_df["POLICY_NUMBER"] == existing_hive_data["POLICY_NUMBER"], "left_anti")
print('------------------Incremental data-----------------------')
incremental_data_df.show()


# 6. counting the number of the new records added to postgres tables
new_records = incremental_data_df.count()
print('------------------COUNTING INCREMENT RECORDS ------------')
print('new records added count', new_records)

# 7.  Adding the incremental_data DataFrame to the existing hive table
# Check if there are extra rows in PostgresSQL. if exist => # write & append to the Hive table
if incremental_data_df.count() > 0:
    # Append new rows to Hive table
    incremental_data_df.write.mode("append").insertInto("{}.{}".format(hive_database_name, hive_table_name))
    print("Appended {} new records to Hive table.".format(incremental_data_df.count()))
else:
    print("No new records been inserted in PostgresSQL table.")


# 8. Stop Spark session
spark.stop()

```

## 3. The provided PySpark code executes the following steps:

### Step 1: Importing Necessary Modules
The code imports essential modules from the PySpark library required for data processing.

### Step 2: Creating Spark Session with Hive Support
A Spark session is created with Hive support enabled, allowing interaction with Hive databases.

### Step 3: Establishing Connection to PostgreSQL and Reading Data
Connection properties for PostgreSQL are defined, and data is read from the PostgreSQL table into a DataFrame.

### Step 4: Determining Incremental Data
The code determines the incremental data by performing a left anti-join between the data from PostgreSQL and the existing data in the Hive table, based on the "POLICY_NUMBER" column.

### Step 5: Counting New Records Added to PostgreSQL Tables
The code counts the number of new records added to the PostgreSQL table, which will be appended to the Hive table.

### Step 6: Adding Incremental Data to Existing Hive Table
If there are new records, they are appended to the existing Hive table. Otherwise, a message indicating no new records have been inserted is displayed.

### Step 7: Stopping Spark Session
The Spark session is stopped to release resources once all data processing tasks are completed.

## 4. GitHub and Jenkins Integration

GitHub serves as the repository for hosting the PySpark code. It allows version control, collaboration, and code sharing.
Jenkins is utilized for Continuous Integration/Continuous Deployment (CI/CD) processes. It monitors GitHub repositories for changes and triggers automated builds and tests whenever new code is pushed.
Jenkins executes the PySpark job defined in the code whenever there's a new commit or code change in the GitHub repository.

link to github  repo : https://github.com/Asadkardame/BD_Colab/blob/hocine_branch/hocine_carInsuranceClaims/pyspark/incrementalLoad.py 

link to pyspark incremental load job  on jenkins : http://3.9.191.104:8080/view/Hocine/job/hocine_pyspark_incremental/


## 5. Explanation
The code reads data from a PostgreSQL database, determines incremental data, and appends it to an existing Hive table.
GitHub ensures version control and collaborative development of the PySpark code.
Jenkins automates the execution of the PySpark job as part of the CI/CD pipeline, ensuring seamless deployment and data processing.

## 6. Conclusion
This documentation outlines the process of incremental data processing using PySpark, emphasizing the integration of GitHub and Jenkins for code hosting and CI/CD orchestration. The provided PySpark code efficiently handles incremental data updates, ensuring data integrity and reliability in the data processing pipeline.



