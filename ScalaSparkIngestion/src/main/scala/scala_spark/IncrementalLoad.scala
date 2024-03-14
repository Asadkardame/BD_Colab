// package scala_spark

// import org.apache.spark.sql.SparkSession

// object IncrementalLoad {
//   def main(args: Array[String]): Unit = {

//     val spark: SparkSession = SparkSession.builder()
//       .master("local[*]")
//       .appName("SparkScala")
//       .enableHiveSupport()
//       .getOrCreate()

//     val maxIdDF = spark.sql("SELECT max(people_id) FROM usukprjdb.people")
//     val maxId = maxIdDF.head().getInt(0).toLong
//     println(maxId)

//     val query = s"""SELECT * FROM people WHERE "people_id" > $maxId"""

//     val moreData = spark.read.format("jdbc")
//       .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
//       .option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").option("query", query).load()
//     moreData = moreData.withColumnRenamed("name", "full_name").withColumnRenamed("age", "current_age")

//     println(moreData.printSchema())
//     println(moreData.show(10))

//     moreData.write.mode("Append").saveAsTable("usukprjdb.people")
//     println("In Hive")
//     println("Success")
//   }}

