package Jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;
public class Splitblot extends BaseBasicBolt {
//会不断的执行，获取队列数据并进行具体操作的方法，接收参数是发过来的spout发出的sentence,
//进行切分
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
String sentence=tuple.getString(0);
        String[] split = sentence.split(" ");
        for (String word : split) {
            word=word.trim();
            if(word.length()!=0){
                basicOutputCollector.emit(new Values(word,1));
            }
        }
    }
//设置字段给下游
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
outputFieldsDeclarer.declare(new Fields("word","num"));
    }
}
