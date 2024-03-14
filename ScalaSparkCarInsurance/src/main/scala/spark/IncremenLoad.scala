package spark

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object IncrementalLoad {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("IncrementalLoad")
      .enableHiveSupport()
      .getOrCreate()

    // Define PostgreSQL connection properties
    val postgresUrl = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
    val postgresProperties = new java.util.Properties()
    postgresProperties.put("user", "consultants")
    postgresProperties.put("password", "WelcomeItc@2022")
    postgresProperties.put("driver", "org.postgresql.Driver")
    val postgresTableName = "car_insurance_claims"

    // Define Hive database and table names
    val hiveDatabaseName = "usukprjdb"
    val hiveTableName = "car_insurance_claims"

    // Read new data from PostgreSQL
    val newData: DataFrame = spark.read.jdbc(url = postgresUrl, table = postgresTableName, properties = postgresProperties)

    // Read existing data from Hive
    val existingHiveData: DataFrame = spark.table(s"$hiveDatabaseName.$hiveTableName")

    // Determine incremental data
    val incrementalDataDF: DataFrame = newData.join(existingHiveData.select("id"), newData("id") === existingHiveData("id"), "left_anti")

    // Write incremental data to Hive table
    incrementalDataDF.write.mode("append").insertInto(s"$hiveDatabaseName.$hiveTableName")

    // Show new records in Hive table
    val newDataHiveDF: DataFrame = spark.sql(s"SELECT * FROM $hiveDatabaseName.$hiveTableName cic WHERE cic.ID = 1 OR cic.ID = 2")
    newDataHiveDF.show()

    // Stop Spark session
    spark.stop()
  }
}
