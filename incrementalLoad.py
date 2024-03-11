from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

class CarInsuranceClaim:
    def __init__(self):
        self.spark = None

    def initialize_spark(self):
        # Create spark session with hive enabled
        self.spark = SparkSession.builder \
            .appName("carInsuranceClaimsApp") \
            .enableHiveSupport() \
            .getOrCreate()

    def close_spark(self):
        # Stop Spark session
        self.spark.stop()

    def process_data(self):
        # 1- Establish the connection to PostgresSQL and hive:
        postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
        postgres_properties = {
            "user": "consultants",
            "password": "WelcomeItc@2022",
            #"driver": "org.postgresql.Driver",
        }
        postgres_table_name = "car_insurance_claims"

        hive_database_name = "project1db"
        hive_table_name = "carinsuranceclaims"

        # Read new dataset from PostgresSQL
        postgres_df = self.spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)

        # 2. Transformations
        postgres_df = postgres_df.withColumnRenamed("ID", "POLICY_NUMBER")
        columns_to_modify = ["MSTATUS", "GENDER", "EDUCATION", "OCCUPATION", "CAR_TYPE", "URBANICITY"]
        for column in columns_to_modify:
            postgres_df = postgres_df.withColumn(column, regexp_replace(col(column), "^z_", ""))

        # 3. Read existing data from Hive table
        existing_hive_data = self.spark.read.table("{}.{}".format(hive_database_name, hive_table_name))

        # 4. Determine the incremental data
        incremental_data_df = postgres_df.join(existing_hive_data.select("POLICY_NUMBER"),
                                                postgres_df["POLICY_NUMBER"] == existing_hive_data["POLICY_NUMBER"],
                                                "left_anti")

        # 5. Add the incremental_data DataFrame to the existing hive table
        new_records = incremental_data_df.count()
        if new_records > 0:
            incremental_data_df.write.mode("append").insertInto("{}.{}".format(hive_database_name, hive_table_name))
            print("Appended {} new records to Hive table.".format(new_records))
        else:
            print("No new records have been inserted in the PostgresSQL table.")

if __name__ == "__main__":
    processor = CarInsuranceClaim()
    processor.initialize_spark()
    processor.process_data()
    processor.close_spark()
