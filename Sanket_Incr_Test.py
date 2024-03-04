import unittest
from pyspark.sql import SparkSession

class TestIncrementalLoad(unittest.TestCase):
    def setUp(self):
        # Initialize SparkSession
        self.spark = SparkSession.builder \
            .appName("TestIncrementalLoad") \
            .master("local[2]") \
            .enableHiveSupport() \
            .config("spark.driver.extraClassPath", "/path/to/postgresql-driver.jar") \ # Replace '/path/to/postgresql-driver.jar' with the actual path to the PostgreSQL JDBC driver JAR file
            .getOrCreate()

    def tearDown(self):
        # Stop SparkSession
        self.spark.stop()

    def test_incremental_load(self):
        # Define test data
        expected_count = 3  # Expected number of rows loaded from PostgreSQL

        # Read updated data from PostgreSQL
        updated_postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
        updated_postgres_properties = {
            "user": "consultants",
            "password": "WelcomeItc@2022",
            "driver": "org.postgresql.Driver",
        }
        updated_postgres_table_name = "updated_health_insurance"
        df_updated_postgres = self.spark.read.jdbc(url=updated_postgres_url, table=updated_postgres_table_name, properties=updated_postgres_properties)

        # Perform incremental data loading to Hive
        hive_database_name = "sanket_db"
        hive_table_name = "health_insurance"
        df_updated_postgres.write.mode('append').saveAsTable(f"{hive_database_name}.{hive_table_name}")

        # Read Hive table
        df_hive = self.spark.read.table(f"{hive_database_name}.{hive_table_name}")

        # Verify the number of rows in Hive after incremental load
        actual_count = df_hive.count()
        self.assertEqual(actual_count, expected_count, "Number of rows in Hive after incremental load does not match expected count")

if __name__ == '__main__':
    unittest.main()
