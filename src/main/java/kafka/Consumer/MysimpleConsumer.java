package kafka.Consumer;



import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @ClassName MysimpleConsumer
 * @Description TODO
 * @Author PL
 * @Date 2018/11/24 14:28
 * @Version 1.0
 * @return
 *
 * 就是可以指定分区，主题，偏移量，来消费具体数据
 **/

public class MysimpleConsumer {



    public static void main(String[] args) {
 //要连的kafka集群
        ArrayList<String>brokers=new ArrayList<String>();
        brokers.add("192.168.91.128");
        brokers.add("192.168.91.130");
        brokers.add("192.168.91.131");
        int port=9092;
        //主题
        String topic="test";
        //分区号
        int partition=1;
        //偏移量
        long offset=0;
//测试
        MysimpleConsumer mysimpleConsumer = new MysimpleConsumer();
        mysimpleConsumer.getData(brokers,port,topic,partition,offset);


    }
           //找到Leader拿到目标Leader分区
    public PartitionMetadata getLeader(List<String> brokers, int port, String topic, int partition){
    for(String broker:brokers){
    SimpleConsumer consumer = new SimpleConsumer(broker, port, 1000, 1024 * 2, "client");
  //获取节点topic元数据信息请求    也可以定义一个List集合，将topic单个添加进去再传进来
    TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
  //获取拿到你传进来的topic元数据信息
    TopicMetadataResponse topicMetadataResponse=consumer.send(topicMetadataRequest);
    List<TopicMetadata> topicMetadata = topicMetadataResponse.topicsMetadata();
  //拿到topic又开始拿每一个topic中的所有分区
    for(TopicMetadata topicMetadata1:topicMetadata){
        List<PartitionMetadata> partitionMetadata = topicMetadata1.partitionsMetadata();
        for (PartitionMetadata partitionMetadatum : partitionMetadata) {
           //如果分区数等于传进来的partition
            if(partitionMetadatum.partitionId()==partition){
                //找到Leader
                return partitionMetadatum;
            }
        }
    }
    consumer.close();
}


        return null;
    }


    public  void getData(List<String> brokers, int port, String topic,int partition,long offset){

        PartitionMetadata partitionMetadata=getLeader(brokers,port,topic,partition);

//拿到Leader节点
        String leader= partitionMetadata.leader().host();
// 拉取数据请求
        SimpleConsumer simpleConsumer = new SimpleConsumer(leader,port,1000,1024*2,"client"+topic);
        FetchRequest request = new FetchRequestBuilder().addFetch(topic, partition, offset, 1024 * 2).build();
        FetchResponse response=simpleConsumer.fetch(request);
        ByteBufferMessageSet messageAndOffsets = response.messageSet(topic, partition);
//获取offset
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            messageAndOffset.offset();
            messageAndOffset.message().toString();
            System.out.println("offset"+messageAndOffset.offset()+"--"+ String.valueOf(messageAndOffset.message().payload().get())

            );

        }
        simpleConsumer.close();

    }


}
