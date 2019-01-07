package kafka.Consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerAPI {
    public static void main(String[] args) {
        Properties props = new Properties();
        //只要能连上kafka集群的节点，都可以
      props.put("bootstrap.servers", "SZ01:9092");
      //消费者组
      props.put("group.id", "test");
      //主动提交
      props.put("enable.auto.commit", "true");
      //提交延时  太短可能会造成进程阻塞
      props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      //KV反序列化
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
     //创建消费者
      KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
    //消费的TOPIC
      consumer.subscribe(Arrays.asList("test","bigdata"));

while(true) {
    ConsumerRecords<String, String> poll = consumer.poll(100);
    for (ConsumerRecord consumerRecord : poll) {
        System.out.println("topic---" + consumerRecord.topic() + "partition---" + consumerRecord.partition() + "offset---" + consumerRecord.offset() + "value---" + consumerRecord.value());
    }
}
   /*   while (true) {
          ConsumerRecords<String, String> records = consumer.poll(100);
          for (ConsumerRecord<String, String> record: records);
              System.out.printf("offset = %d, key = %s, value = %s%n",record.off, record.key(), record.value());
      }*/

    }
}
