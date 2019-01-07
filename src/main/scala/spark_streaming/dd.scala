



import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 对接kafka数据源
  */

object KafkaSparkStreaming {
  def main(args: Array[String]): Unit = {


    //初始化ssc
    val conf = new SparkConf().setAppName("KafkaSparkStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(3))

    //定义kafka的一些参数
    val zk = "SZ01:2181,SZ03:2181,SZ04:2181"
    val brokers = "SZ01:9092,SZ03:9092,SZ04:9092"
    val topic = "first"
    val group = "g"

    //将kafka的参数封装到Map集合中
    val kafkaParam: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "zookeeper.connect" -> zk
    )

    //读取kafka数据创建DStream   注意泛型 createStream
    //新旧API不一样
    val kafkaDStream: ReceiverInputDStream[(String, String)] =
    KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc,
      kafkaParam,
      Map[String, Int](topic -> 3), //话题下还有分区
      StorageLevel.MEMORY_ONLY
    )


    kafkaDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
