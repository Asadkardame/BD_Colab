import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan, ResultScanner}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes

println("#####################################################")
println( "The Program is running.." )
println("#####################################################")

println("Connecting to HBase...")

// HBase configuration
val hbaseConf = HBaseConfiguration.create()
hbaseConf.set("hbase.zookeeper.quorum", "ip-172-31-3-80.eu-west-2.compute.internal,ip-172-31-5-217.eu-west-2.compute.internal,ip-172-31-9-237.eu-west-2.compute.internal")
hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

val connection = ConnectionFactory.createConnection(hbaseConf)
val tableName = TableName.valueOf("Insurance_Claim_11_3_11h_50")
val table = connection.getTable(tableName)

// Scan the table to count the rows
val scan = new Scan()
val scanner: ResultScanner = table.getScanner(scan)
var rowCount: Int = 0

try {
  println("Counting rows...")
  val iterator = scanner.iterator()
  while (iterator.hasNext) {
    val result = iterator.next()
    if (!result.isEmpty) {
      rowCount += 1
    }
  }
} finally {
  scanner.close()
}

print("################## RESULT #####################")
println(s"Total number of rows in the table: $rowCount")
print("################## RESULT #####################")

