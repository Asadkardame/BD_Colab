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
cur = conn.cursor()

# Execute SQL query to find dates outside a valid range
cur.execute("SELECT * FROM people WHERE your_date_column < 'start_date' OR your_date_column > 'end_date'")
invalid_dates = cur.fetchall()
print("Dates outside valid range:", invalid_dates)

# Execute SQL query to find dates with invalid formats
cur.execute("SELECT * FROM your_table WHERE NOT your_date_column::text ~ '^\d{4}-\d{2}-\d{2}$'")
invalid_formats = cur.fetchall()
print("Dates with invalid formats:", invalid_formats)

# Close cursor and connection
cur.close()
conn.close()
