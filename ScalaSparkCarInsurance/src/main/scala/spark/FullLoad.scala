package spark

import org.apache.spark.sql.SparkSession


object FullLoad{

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("MiniPrjScala").enableHiveSupport().getOrCreate()
    val df = spark.read.format("jdbc").option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("dbtable", "car_insurance_claims").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").load()
    println(df.printSchema())
    println(df.show(10))
    println("Automated")

    // Renaming column "age" to "current_age"
    // Transformer
    val renamedDf = df.withColumnRenamed("AGE", "CURRENT_AGE")

    renamedDf.write.mode("overwrite").saveAsTable("usukprjdb.car_insurance_claims")
    println("In Hive")
  }}