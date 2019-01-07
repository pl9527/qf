package kafka.Producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;



public class Producer生产者API {
    public static void main(String[] args) {
    // 复制源码案例部分
     Properties props = new Properties();
     //kafka集群
     props.put("bootstrap.servers", "SZ01:9092");
     props.put("acks", "all");
     //重试次数
     props.put("retries", 0);
     //缓存大小 ，多少条数据
     props.put("batch.size", 16384);
     //发送消息延时
     props.put("linger.ms", 1);
     //内存大小
     props.put("buffer.memory", 33554432);
     //props.put("partitioner.class", "kafka.Producer.自定义分区类" );
     //KV序列化
     props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
     props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
     Producer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("test", String.valueOf(i), String.valueOf(i)), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println(metadata.partition()+"---"+metadata.offset());
                }
            });//向哪个分区发送消息
        }
        producer.close();
    }
}
