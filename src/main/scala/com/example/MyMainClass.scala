package com.example

import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Admin}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, HColumnDescriptor, HTableDescriptor}

object MyMainClass extends App{

//####################################################
// Parameters for the script
val url   = "http://18.133.73.36:5001/insurance_claims1" // API URL to fetch the JSON data
val topic = "insurance_claims_5-3-12-47" // Kafka topic name & table name in HBase (or Hive)
//####################################################

// Initialize Spark session for DataFrame and Dataset APIs
val spark = SparkSession.builder.appName("API to Kafka _ 2").getOrCreate()

// Correctly import Spark SQL implicits
import spark.implicits._

// Fetch data from URL and convert it to a string
val result = Source.fromURL(url).mkString

// Read the JSON data into a DataFrame
val jsonData = spark.read.json(Seq(result).toDS)
jsonData.show() // Display the DataFrame contents

// Kafka servers configuration
val kafkaServers = "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092"

// Publish the DataFrame as JSON string to the specified Kafka topic
jsonData.selectExpr("to_json(struct(*)) AS value").write.format("kafka").option("kafka.bootstrap.servers", kafkaServers).option("topic", topic).save()

// Read from the Kafka topic to verify the data pipeline
val df = (spark
   .read
   .format("kafka")
   .option("kafka.bootstrap.servers", kafkaServers)
   .option("subscribe", topic)
   .option("startingOffsets", "earliest")
   .load())

// Convert Kafka messages to string for processing
val messages = df.selectExpr("CAST(value AS STRING) as message").as[String]

// Configure HBase connection
val hbaseConf = HBaseConfiguration.create()
hbaseConf.set("hbase.zookeeper.quorum", "ip-172-31-3-80.eu-west-2.compute.internal,ip-172-31-5-217.eu-west-2.compute.internal,ip-172-31-9-237.eu-west-2.compute.internal")
hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
hbaseConf.set("zookeeper.znode.parent", "/hbase")

// Establish HBase connection
val connection = ConnectionFactory.createConnection(hbaseConf)

// Prepare HBase table for storing the messages
val tableName = TableName.valueOf(topic)
val columnFamilyName = "cf"
val admin = connection.getAdmin

// Check if table exists, if not, create it
if (!admin.tableExists(tableName)) {
   val tableDescriptor = new HTableDescriptor(tableName)
   val columnDescriptor = new HColumnDescriptor(columnFamilyName)
   tableDescriptor.addFamily(columnDescriptor)
   admin.createTable(tableDescriptor)
   println(s"Table $tableName created.")
}

// Get the HBase table
val table = connection.getTable(tableName)

// Function to generate unique row keys for messages
def generateUniqueRowKey(message: String): String = {
   val currentTime = System.currentTimeMillis()
   val messageHash = message.hashCode
   s"$currentTime-$messageHash"
}

// Insert each Kafka message into HBase
messages.collect().foreach { message =>
   val rowKey = generateUniqueRowKey(message)
   val put = new Put(Bytes.toBytes(rowKey))
   put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("column"), Bytes.toBytes(message))
   table.put(put)
}

// Print summary of operations
println("----------------------------")
println( "It was the API  : " + url   ) 
println( "Kafka Topic was : " + topic )
println( "HBase table is  : " + table )
println("----------------------------")

// Close HBase table and connection to clean up resources
table.close()
connection.close()

}
 
