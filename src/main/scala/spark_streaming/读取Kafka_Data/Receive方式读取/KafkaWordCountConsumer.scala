package spark_streaming.读取Kafka_Data.Receive方式读取

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaWordCountConsumer {
  def main(args: Array[String]): Unit = {
    //zookeeper  group topic
if(args.length<4){
  System.err.println("参数不正确")
  System.exit(1)
}
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("E://checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordCounts: DStream[(String, Long)] = words.map(x => (x, 1L)).reduceByKeyAndWindow(_+_,_+_,Seconds(10), Seconds(2))


    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()




  }
}
