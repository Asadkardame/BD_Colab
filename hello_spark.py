from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType



# Initialize SparkSession
spark = SparkSession.builder.appName("RandomDataFrame").getOrCreate()

# Define schema for DataFrame
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("value", IntegerType(), True)
])

# Generate random data
data = [(i, i * 2) for i in range(10)]

# Create DataFrame from random data
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()

# # Stop SparkSession
# spark.stop()

print("Hello World!!")