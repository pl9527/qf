package Jstorm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class Aggrblot extends BaseBasicBolt {
//用来存储结果数据，key=单词，value=单词出现的次数
    Map<String,Integer> map =new HashMap<String,Integer>();

    public void execute(Tuple input, BasicOutputCollector basicOutputCollector) {
        String word = String.valueOf(input.getValueByField("word"));
        int num=(Integer)input.getValueByField("num");
if(!map.containsKey(word)){
    map.put(word,num);
}
else{
    map.put(word,map.get(word)+1);

}
        System.out.println("统计结果为：" + map);
    }
//此时处理完毕，不需要再发射数据给别的地方
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
