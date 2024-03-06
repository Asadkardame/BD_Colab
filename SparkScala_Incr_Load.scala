package scalatest;
import org.apache.spark.sql.SparkSession

object IncrLoad {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("IncrementalLoadTest")
      // .master("local[*]") // Use local mode for testing
      .getOrCreate()

    // Define PostgreSQL connection properties
    val postgresUrl = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
    val postgresProperties = new java.util.Properties()
    postgresProperties.put("user", "consultants")
    postgresProperties.put("password", "WelcomeItc@2022")
    postgresProperties.put("driver", "org.postgresql.Driver")

    try {
      // Read existing data from Hive table
      val existingData = spark.sql("SELECT * FROM people")

      // Read new data from PostgreSQL
      val newData = spark.read.jdbc(postgresUrl, "people", postgresProperties)

      // Identify new rows by performing a left anti join
      val incrementalData = newData.join(existingData, newData.columns, "left_anti")
      incrementalData.show()
      if (newData.count() == incrementalData.count() + existingData.count()){
        println("Count Matches")
      }
      if (incrementalData.isEmpty) {
        println("No new data to load. Incremental load test passed.")
      } else {
        // Append new data to Hive table
        incrementalData.write.mode("append").saveAsTable("people")
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
