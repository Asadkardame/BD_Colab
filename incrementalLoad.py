from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace


def IncrLoad(postgres_url, postgres_properties, postgres_table_name, hive_data):
    # Create spark session with hive enabled
    spark = SparkSession.builder \
        .appName("PySpark_Incr_Load") \
        .enableHiveSupport() \
        .getOrCreate()

    # Read data from PostgresSQL
    df_postgres = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
    df_postgres.show(3)
    
    
    columns_to_modify = ["MSTATUS", "GENDER", "EDUCATION", "OCCUPATION", "CAR_TYPE", "URBANICITY"]
    for column in columns_to_modify:
        df_postgres = df_postgres.withColumn(column, regexp_replace(col(column), "^z_", ""))


    

    hive_database_name = hive_data['hive_database_name']
    hive_table_name = hive_data['hive_table_name']

    # Read new dataset from PostgresSQL
    

    # 2. Transformations
    df_postgres = df_postgres.withColumnRenamed("ID", "POLICY_NUMBER")
   

    # 3. Read existing data from Hive table
    existing_hive_data = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))

    # 4. Determine the incremental data
    incremental_data_df = df_postgres.join(existing_hive_data.select("POLICY_NUMBER"),
                                            df_postgres["POLICY_NUMBER"] == existing_hive_data["POLICY_NUMBER"],
                                            "left_anti")

    # 5. Add the incremental_data DataFrame to the existing hive table
    new_records = incremental_data_df.count()
    if new_records > 0:
        incremental_data_df.write.mode("append").insertInto("{}.{}".format(hive_database_name, hive_table_name))
        print("Appended {} new records to Hive table.".format(new_records))
    else:
        print("No new records have been inserted in the PostgresSQL table.")


if __name__ == "__main__":
    postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
    postgres_properties = {
        "user": "consultants",
        "password": "WelcomeItc@2022",
        "driver": "org.postgresql.Driver",
    }
    postgres_table_name = "car_insurance_claims"
    
    hive_data = {
        "hive_database_name": "project1db",
        "hive_table_name": "carInsuranceClaims"
    }
    IncrLoad(postgres_url, postgres_properties, postgres_table_name, hive_data)
