package spark_streaming.读取Kafka_Data.Receive方式读取

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

object KafkaWordCountProducer {
  def main(args: Array[String]): Unit = {
    if(args.length<2){
      System.err.println("参数不全")
      System.exit(1)
    }
    val Array(brokers,topic)=args
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer=new KafkaProducer[String,String](props)

var arr=Array("hello tom","hello jerry","hello tom","hello penglin")
    var r=new Random()
    while(true){
      val message=arr(r.nextInt(arr.length))
      producer.send(new ProducerRecord[String,String](topic,message))
      Thread.sleep(1000)
    }


  }
}
