

import unittest


from pyspark.sql import SparkSession
from Pyspark_Full_Load import TestFullDataLoading
from Pyspark_Incr_Load import TestIncrDataLoading

if __name__ == "__main__":


    ######################Test Pyspark Full load #############################

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("TestFullDataLoading") \
        .master("local[2]") \
        .enableHiveSupport() \
        .getOrCreate()

    # Load the test cases from TestDataLoading
    suite = unittest.TestLoader().loadTestsFromTestCase(TestFullDataLoading)

    # Run the tests
    unittest.TextTestRunner().run(suite)

    # Stop SparkSession
    spark.stop()

    #####################Test Pyspark Incremental load #########################

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("TestIncrDataLoading") \
        .master("local[2]") \
        .enableHiveSupport() \
        .getOrCreate()

    # Load the test cases from TestDataLoading
    suite = unittest.TestLoader().loadTestsFromTestCase(TestIncrDataLoading)

    # Run the tests
    unittest.TextTestRunner().run(suite)

    # Stop SparkSession
    spark.stop()


