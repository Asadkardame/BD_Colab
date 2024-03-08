import unittest
from pyspark.sql import SparkSession

class Pyspark_Incr_Load(unittest.TestCase):
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
        initial_count_df = self.spark.sql("SELECT COUNT(*) AS count FROM sanket_db.health_insurance")
        initial_count = initial_count_df.collect()[0]["count"]

        # Read data from PostgreSQL
        postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
        postgres_properties = {
            "user": "consultants",
            "password": "WelcomeItc@2022",
            "driver": "org.postgresql.Driver",
        }
        # postgres_table_name = "health_insurance"
        # # where_condition = "BeneID = 'BENE177334'"
        # df_postgres = self.spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
        # # df_postgres = self.spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties,
        # #                             column=None,  # Specify None for column
        # #                             lowerBound=None,  # Specify None for lowerBound
        # #                             upperBound=None,  # Specify None for upperBound
        # #                             numPartitions=None,  # Specify None for numPartitions
        # #                             predicates=[where_condition])  # Use predicates parameter for WHERE condition

        whereCondition = "POLICY_NUMBER >= 1"
        # Define the query with the WHERE condition
        query = s"(SELECT * FROM car_insurance_claims1 WHERE $whereCondition) AS data"

        # Read new data from PostgreSQL with the WHERE condition
        newData = spark.read \
        .jdbc(postgresUrl, query, properties=postgresProperties)


        # Perform data loading to Hive
        df_postgres.write.mode('overwrite').saveAsTable("carinsuranceclaims")
        # df_postgres.show()
        
        # Read count of rows from Hive table after incremental load
        updated_count_df = self.spark.sql("SELECT COUNT(*) AS count FROM carinsuranceclaims")
        updated_count = updated_count_df.collect()[0]["count"]

        # Compute expected count
        # df_postgres.count.show()
        expected_count = initial_count + df_postgres.count()
        post_gres_count = df_postgres.count()
        print("Count of df_postgres:", post_gres_count)
        print("Count of initial:", initial_count)
        print("Count of updated_count:", updated_count)


        
        # Verify the number of rows loaded to Hive
        self.assertEqual(updated_count, post_gres_count, "Number of rows loaded to Hive after incremental load does not match expected count")

if __name__ == '__main__':
    unittest.main()
