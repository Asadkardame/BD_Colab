def test_incremental_load(self):
    # Read initial count of rows from Hive table
    hive_database_name = "sanket_db"
    hive_table_name = "health_insurance"
    # initial_count_df = self.spark.sql(f"SELECT COUNT(*) AS count FROM project1db.carinsuranceclaims")
    initial_count_df = self.spark.sql("SELECT COUNT(*) AS count FROM project1db.carinsuranceclaims")

    initial_count = initial_count_df.collect()[0]["count"]

    # Read data from PostgreSQL
    postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
    postgres_properties = {
        "user": "consultants",
        "password": "WelcomeItc@2022",
        "driver": "org.postgresql.Driver",
    }
    whereCondition = "POLICY_NUMBER = 1"
    # Define the query with the WHERE condition
    query = f"(SELECT * FROM car_insurance_claims1 WHERE {whereCondition}) AS data"

    # Read new data from PostgreSQL with the WHERE condition
    newData = self.spark.read.jdbc(postgres_url, query, properties=postgres_properties)

    # Perform data loading to Hive
    newData.write.mode('overwrite').saveAsTable("project1db.carinsuranceclaims")
    
    # Read count of rows from Hive table after incremental load
    updated_count_df = self.spark.sql("SELECT COUNT(*) AS count FROM project1db.carinsuranceclaims")
    updated_count = updated_count_df.collect()[0]["count"]

    # Compute expected count
    expected_count = initial_count + newData.count()

    # Verify the number of rows loaded to Hive
    self.assertEqual(updated_count, expected_count, "Number of rows loaded to Hive after incremental load does not match expected count")
