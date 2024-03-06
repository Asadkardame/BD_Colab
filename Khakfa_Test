import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, FlatSpec}
import org.mockito.ArgumentMatchers._

class TestAPIReadingAndKafkaCreation extends FlatSpec with BeforeAndAfter {
  var spark: SparkSession = _

  before {
    // Initialize SparkSession
    spark = SparkSession.builder()
      .appName("TestAPIReadingAndKafkaCreation")
      .master("local[2]")
      .getOrCreate()
  }

  after {
    // Stop SparkSession
    spark.stop()
  }

  "APIReadingAndKafkaCreation" should "read API and create Kafka topic" in {
    // Mock the DataFrameReader
    val mockDataFrameReader = mock(classOf[DataFrameReader])
    when(spark.read).thenReturn(mockDataFrameReader)

    // Mock the API response
    val mockApiResponse = Seq(
      (1, "John Doe", 30),
      (2, "Jane Smith", 35)
    )
    val mockDataFrame = spark.createDataFrame(mockApiResponse).toDF("id", "name", "age")
    when(mockDataFrameReader.json("http://18.133.73.36:5001/insurance_claims1")).thenReturn(mockDataFrame)

    // Mock the DataFrameWriter
    val mockDataFrameWriter = mock(classOf[DataFrameWriter])
    when(mockDataFrame.write).thenReturn(mockDataFrameWriter)
    when(mockDataFrameWriter.mode(anyString())).thenReturn(mockDataFrameWriter)
    when(mockDataFrameWriter.format(anyString())).thenReturn(mockDataFrameWriter)

    // Call the function to read API and create Kafka topic
    // Replace the function name and arguments with your actual implementation
    // For example:
    // functionToReadAPIAndCreateKafkaTopic()

    // Verify that the API was read and Kafka topic was created
    verify(mockDataFrameReader).json("http://18.133.73.36:5001/insurance_claims1")
    verify(mockDataFrameWriter).saveAsTable("kafka_topic")
  }
}
