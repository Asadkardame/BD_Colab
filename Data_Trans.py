from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Count_Comparison_After_Transformation") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
    .enableHiveSupport() \
    .getOrCreate()

# Read data from PostgreSQL
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
        postgres_properties = {
            "user": "consultants",
            "password": "WelcomeItc@2022",
            "driver": "org.postgresql.Driver",
        }

# Read data from Hive
hive_df = spark.sql("SELECT * FROM usukprjdb.people")

# Compare counts
postgres_count = postgres_df.count()
hive_count = hive_df.count()

print("Count after transformation:")
print("PostgreSQL count:", postgres_count)
print("Hive count:", hive_count)

if postgres_count == hive_count:
    print("Counts match between PostgreSQL and Hive after transformation.")
else:
    print("Counts do not match between PostgreSQL and Hive after transformation.")

# Stop SparkSession
spark.stop()
