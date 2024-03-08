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
def testIncrLoad(self):
    # Read initial count of rows from Hive table
    hive_database_name = "project1db"
    hive_table_name = "carinsuranceclaims"
    # initial_count_df = self.spark.sql(f"SELECT COUNT(*) AS count FROM project1db.carinsuranceclaims")
    initial_count_df = self.spark.sql("SELECT COUNT(*) AS count FROM project1db.carinsuranceclaims")

    initial_count = initial_count_df.collect()[0]["count"]
    print("Initial_count")

    # Read data from PostgreSQL
    postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
    postgres_properties = {
        "user": "consultants",
        "password": "WelcomeItc@2022",
        "driver": "org.postgresql.Driver",
    }
    whereCondition = "POLICY_NUMBER = 3"
    # Define the query with the WHERE condition
    # query = f"(SELECT * FROM car_insurance_claims1 WHERE {whereCondition}) AS data"
    query = "(SELECT * FROM car_insurance_claims1 WHERE " + whereCondition + ") AS data"
    print("Generated SQL query:", query)


    # Read new data from PostgreSQL with the WHERE condition
    newData = self.spark.read.jdbc(postgres_url, query, properties=postgres_properties)
    newData.show()
    # Perform data loading to Hive
    newData.write.mode('overwrite').saveAsTable("project1db.carinsuranceclaims")
    
    # Read count of rows from Hive table after incremental load
    updated_count_df = self.spark.sql("SELECT COUNT(*) AS count FROM project1db.carinsuranceclaims")
    updated_count = updated_count_df.collect()[0]["count"]

    # Compute expected count
    expected_count = initial_count + newData.count()

    # Verify the number of rows loaded to Hive
    self.assertEqual(updated_count, expected_count, "Number of rows loaded to Hive after incremental load does not match expected count")

if __name__ == '__main__':
    unittest.main()
