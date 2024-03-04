import unittest
from pyspark.sql import SparkSession

class TestDataLoading(unittest.TestCase):
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

    def test_incremental_load(self):
        # Read initial count of rows from Hive table
        hive_database_name = "sanket_db"
        hive_table_name = "health_insurance"
        initial_count_df = self.spark.sql(f"SELECT COUNT(*) AS count FROM sanket_db.health_insurance")
        initial_count = initial_count_df.collect()[0]["count"]

        # Read data from PostgreSQL
        postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
        postgres_properties = {
            "user": "consultants",
            "password": "WelcomeItc@2022",
            "driver": "org.postgresql.Driver",
        }
        postgres_table_name = "health_insurance"
        df_postgres = self.spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)

        # Perform data loading to Hive
        df_postgres.write.mode('append').saveAsTable("sanket_db.health_insurance")

        # Read count of rows from Hive table after incremental load
        updated_count_df = self.spark.sql("SELECT COUNT(*) AS count FROM sanket_db.health_insurance")
        updated_count = updated_count_df.collect()[0]["count"]

        # Compute expected count
        expected_count = initial_count + df_postgres.count()

        # Verify the number of rows loaded to Hive
        self.assertEqual(updated_count, expected_count, "Number of rows loaded to Hive after incremental load does not match expected count")

if __name__ == '__main__':
    unittest.main()
