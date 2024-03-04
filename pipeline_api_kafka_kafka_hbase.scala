//##################################################################
//          Scala-Spark Pipeline : API-KAFKA-KAFKA-HBASE
//##################################################################

// =================================================================
// Here the pure scala-spark script :
// -----------------------------------------------------------------

import org.apache.spark.sql.SparkSession
import scala.io.Source

val spark = SparkSession.builder.appName("API to Kafka").getOrCreate()

import spark.implicits._

val url = "http://18.133.73.36:5001/insurance_claims1"

val result = Source.fromURL(url).mkString

val jsonData = spark.read.json(Seq(result).toDS)

jsonData.show()



val kafkaServers = "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092"
kafkaServers: String = ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092

val topic = "insurance_claims_33"

jsonData.selectExpr("to_json(struct(*)) AS value").write.format("kafka").option("kafka.bootstrap.servers", kafkaServers).option("topic", topic).save()

val df = (spark
   .read
   .format("kafka")
   .option("kafka.bootstrap.servers", kafkaServers)
   .option("subscribe", topic)
   .option("startingOffsets", "earliest")
   .load())

val messages = df.selectExpr("CAST(value AS STRING) as message").as[String]

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

val hbaseConf = HBaseConfiguration.create()

hbaseConf.set("hbase.zookeeper.quorum", "ip-172-31-3-80.eu-west-2.compute.internal,ip-172-31-5-217.eu-west-2.compute.internal,ip-172-31-9-237.eu-west-2.compute.internal")

hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
hbaseConf.set("zookeeper.znode.parent", "/hbase")

val connection = ConnectionFactory.createConnection(hbaseConf)

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Admin}
import org.apache.hadoop.hbase.util.Bytes

val tableName = TableName.valueOf("insurance_claims_33")
val columnFamilyName = "cf"
val admin = connection.getAdmin


if (!admin.tableExists(tableName)) {
   val tableDescriptor = new HTableDescriptor(tableName)
   val columnDescriptor = new HColumnDescriptor(columnFamilyName)
   tableDescriptor.addFamily(columnDescriptor)
   admin.createTable(tableDescriptor)
   println(s"Table $tableName created.")
 }

val table = connection.getTable(tableName)

messages.collect().foreach { message =>
   // Here, you might want to generate a unique row key for each message
   val rowKey = generateUniqueRowKey(message) // Implement this function based on your requirements
   val put = new Put(Bytes.toBytes(rowKey))
   put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("column"), Bytes.toBytes(message))
   table.put(put)
 }

def generateUniqueRowKey(message: String): String = {
   // Simple example: Use current time and hash of message
   val currentTime = System.currentTimeMillis()
   val messageHash = message.hashCode
   s"$currentTime-$messageHash"
 }

val table = connection.getTable(tableName)
