import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.streaming.Trigger


object KafkaConsumerTweets {
  def main(args: Array[String]): Unit = {

    val nullAppender = new NullAppender

    BasicConfigurator.configure(nullAppender)

    val spark = SparkSession
      .builder
      .config("es.nodes", "127.0.0.1")
      .config("es.port", "9200")
      .master("local[8]")
      .appName("StructuredNetworkTweets")
      .getOrCreate()
    import spark.implicits._

    spark.conf.set("spark.sql.shuffle.partitions", 2)

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test2")
      .load()

    val df1 = df.selectExpr("CAST(value AS STRING)")
      .select(functions.json_tuple($"value", "id", "date", "user", "text", "retweets"))
      .toDF("id", "date", "user", "text", "retweets")

    val query = df1.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    df1.writeStream
      .outputMode("append")
      .format("mongo") // Use the MongoDB connector
      .option("checkpointLocation", "[path_of_checkpointLoc_logs]") //?????
      .option("uri", "mongodb://127.0.0.1:27017") // MongoDB URI
      .option("database", "Tweets") // MongoDB database name
      .option("collection", "tweets") // MongoDB collection name
      .start("[index_name]") //???
      .awaitTermination()
  }
}
