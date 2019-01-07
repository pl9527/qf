package kafka.Producer;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @ClassName 自定义分区类
 * @Description TODO
 * @Author PL
 * @Date 2018/11/24 10:49
 * @Version 1.0
 * @return
 **/

public class 自定义分区类 implements Partitioner {


    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
