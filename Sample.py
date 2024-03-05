import unittest
from pyspark.sql import SparkSession

class TestIncrDataLoading(unittest.TestCase):
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
        # Read existing data from Hive table
        existing_data = self.spark.sql("SELECT * FROM sanket_db.health_insurance")

        # Read new data from PostgreSQL
        postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
        postgres_properties = {
            "user": "consultants",
            "password": "WelcomeItc@2022",
            "driver": "org.postgresql.Driver",
        }
        postgres_table_name = "health_insurance"
        new_data = self.spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)

        # Identify new rows
        new_rows = new_data.join(existing_data, new_data.columns, "left_anti")

        # Append new rows to Hive table
        if not new_rows.rdd.isEmpty():
            new_rows.write.mode('append').saveAsTable('sanket_db.health_insurance')
            print("New rows inserted into Hive for incremental load.")
        else:
            print("No new rows to insert.")
        
        # Verify the number of rows loaded to Hive
        updated_count_df = self.spark.sql("SELECT COUNT(*) AS count FROM sanket_db.health_insurance")
        updated_count = updated_count_df.collect()[0]["count"]
        expected_count = existing_data.count() + new_rows.count()
        new_rows1 = new_rows.count
        exp_rows1 = expected_data.count
        print('updated_count', updated_count)
        print('expected_count', expected_count)
        print('New_count', new_rows1)
        print('New_count', exp_rows1)
        self.assertEqual(updated_count, expected_count, "Number of rows loaded to Hive after incremental load does not match expected count")

if __name__ == '__main__':
    unittest.main()
