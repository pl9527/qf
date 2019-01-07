package Jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class RandomgetDataspout extends BaseRichSpout {
   //tuple发射器
    private SpoutOutputCollector collector;
    Random random;


   //spout组件的初始化方法。创建实例时只调用一次
    //map包含storm配置信息
    //TopologyContext对象提供了topology中组件的信息
    //SpoutOutputCollector对象提供了tuple的发射方法
    public void open(Map config, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
      this.collector=spoutOutputCollector;
      random=new Random();
    }
    //通过这个方法向输出的collector队列中发射tuple，被循环用到
    public void nextTuple() {
        String[] sentences={
                "my dog has fleas",
                "i like cold beverages",
                "the dog ate my homework",
                "don't hava a cow man ",
                "i don't think i like fleas"
        };
String sentence=sentences[random.nextInt(sentences.length)];
collector.emit(new Values(sentence));
    }
//用于给发射的数据定义字段名称 给下游，消息源可以发射多条消息流stream,多条消息流可以理解为多种类型的数据
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
outputFieldsDeclarer.declare(new Fields("sentence"));//随便给，下游根据这个来获取
    }
}
