import org.apache.spark.sql.SparkSession

object HiveExternalTableCreation extends App {
  // Initialize SparkSession with Hive Support
  val spark = SparkSession.builder()
    .appName("Hive External Table Creation")
    .enableHiveSupport()
    .getOrCreate()

  // Your Hive database and table name
  val databaseName = "test_db"
  val tableName = "scala_person"
  val tableLocation = "'hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/USUK30/Asad/hive'" // HDFS or local path to store table data

  // SQL statement to create an external database
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")

  // SQL statement to create an external table
  spark.sql(s"""
      |CREATE EXTERNAL TABLE IF NOT EXISTS $databaseName.$tableName (
      |  id INT,
      |  data STRING
      |)
      |STORED AS PARQUET
      |LOCATION $tableLocation
      |""".stripMargin)

  spark.stop()
}
