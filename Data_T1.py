import unittest
from pyspark.sql import SparkSession

class TestFullDataLoading(unittest.TestCase):
    def setUp(self):
        # Initialize SparkSession
        self.spark = SparkSession.builder \
            .appName("TestDataLoading") \
            .master("local[2]") \
            .enableHiveSupport() \
            .getOrCreate()

    def tearDown(self):
        # Stop SparkSession
        self.spark.stop()

    def test_data_loading(self):
        # Read data from PostgreSQL
        postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
        postgres_properties = {
            "user": "consultants",
            "password": "WelcomeItc@2022",
            "driver": "org.postgresql.Driver",
        }
        postgres_table_name = "people"
        df_postgres = self.spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
        
        # Fetch column names from the PostgreSQL DataFrame
        postgres_columns = df_postgres.columns

        # Rename columns to match Hive column names dynamically
        renamed_columns = [column.lower().replace(" ", "_") for column in postgres_columns]

        df_postgres_transformed = df_postgres
        for index, column in enumerate(postgres_columns):
            df_postgres_transformed = df_postgres_transformed.withColumnRenamed(column, renamed_columns[index])

        # Count rows loaded from PostgreSQL
        Postgres_count = df_postgres_transformed.count()

        # Read Hive table
        df_hive = self.spark.read.table("usukprjdb.people")

        # Count rows loaded to Hive
        Hive_count = df_hive.count()

        # Verify the number of rows loaded to Hive
        if Postgres_count == Hive_count:
            print("Number of rows loaded to Hive matches the expected count")
            print('Postgres_Count', Postgres_count)
            print('Hive_count', Hive_count)
        else:
            self.assertEqual(Hive_count, Postgres_count, "Number of rows loaded to Hive does not match expected count")

if __name__ == '__main__':
    unittest.main()