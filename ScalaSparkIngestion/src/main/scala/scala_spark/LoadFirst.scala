package scala_spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LoadFirst {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("MiniPrjScala").enableHiveSupport().getOrCreate()
    val df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb").option("dbtable", "people").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "welcomeitc@2022").load()
    println(df.printSchema())
    println(df.show(10))
    println("Automated")

    // Define the calculation of age
    //val df_age = df.withColumn("DOB", to_date(col("DOB"), "M/d/yyyy")).withColumn("age", floor(datediff(current_date(), col("DOB")) / 365))

    df.show(10)


    df.write.mode( "overwrite").saveAsTable("usukprjdb.testscalaspark")
    println("In Hive")
  }