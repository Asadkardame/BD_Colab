package scalatest

import org.apache.spark.sql.SparkSession

object SparkScala_Incr_Load {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("IncrementalLoadTest")
      .getOrCreate()

    // Define PostgreSQL connection properties
    val postgresUrl = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
    val postgresProperties = new java.util.Properties()
    postgresProperties.put("user", "consultants")
    postgresProperties.put("password", "WelcomeItc@2022")
    postgresProperties.put("driver", "org.postgresql.Driver")

    try {
      // Read existing data from Hive table
      val existingData = spark.read.format("parquet").table("project1db.carinsuranceclaims") // Read the existing table directly

      // Read new data from PostgreSQL
      // val newData = spark.read.jdbc(postgresUrl, "car_insurance_claims", postgresProperties)
      
      val whereCondition = """"POLICY_NUMBER" = 2"""
      // Read new data from PostgreSQL with the WHERE condition
      // val newData = spark.read.jdbc(postgresUrl, "car_insurance_claims", postgresProperties, predicates = Array(whereCondition))
      // val newData = spark.read.jdbc(postgresUrl, "car_insurance_claims", postgresProperties, predicates = Map("predicates" -> whereCondition))
      // val newData = spark.read.jdbc(s"$postgresUrl?user=consultants&password=WelcomeItc@2022&$whereCondition", "car_insurance_claims", postgresProperties)
      val query = s"(SELECT * FROM car_insurance_claims WHERE $whereCondition) AS data"
      val newData = spark.read.jdbc(postgresUrl, query, postgresProperties)

      newData.show()



      // Identify new rows by performing a left anti join
      // val incrementalData = newData.join(existingData, newData.columns, "left_anti")
      val incrementalData = newData
      // incrementalData.show()
      println("New_Data", newData.count()) 
      println("Existing_Data", existingData.count())
      if (newData.count() == incrementalData.count() + existingData.count()){
        println("Count Matches")
      }

      if (incrementalData.isEmpty) {
        println("No new data to load. Incremental load test passed.")
      } else {
        // Append new data to Hive table
        incrementalData.write.mode("append").format("parquet").saveAsTable("project1db.carinsuranceclaims")
        println("Incremental load successful.")
      }
    } catch {
      case e: Exception =>
        println(s"Test failed: ${e.getMessage}")
    } finally {
      // Stop SparkSession after testing
      spark.stop()
    }
  }
}
