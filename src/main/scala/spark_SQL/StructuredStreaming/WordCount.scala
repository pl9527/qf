package spark_SQL.StructuredStreaming

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object WordCount {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("d").master("local").getOrCreate()
    import session.implicits._

    val frame: DataFrame = session.readStream.format("socket").option("host", "SZ01")
      .option("port", "6666").load()

    val un: Dataset[String] = frame.as[String].flatMap(_.split(" "))

    val f1: DataFrame = un.groupBy("value").count()
//
    val query: StreamingQuery = f1.writeStream.outputMode("complete")
      .format("console").start()
    query.awaitTermination()


  }
}
