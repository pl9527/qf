package Jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

//创建驱动类实现单词计数
//分3个部分-- spout获取数据  blot1 切分数据 blot2聚合数据
public class WordCountTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
//初始化环境，调用拓扑构建类
        TopologyBuilder topologyBuilder=new TopologyBuilder();
//创建自定义spout,并调用来获取数据
        topologyBuilder.setSpout("randomgetdataspout",new RandomgetDataspout(),1);
//创建自定义blot,并调用来切分数据
        topologyBuilder.setBolt("splitblot",new Splitblot(),2).shuffleGrouping("randomgetdataspout");
        topologyBuilder.setBolt("aggrblot",new Aggrblot(),2).fieldsGrouping("splitblot",new Fields("word"));
//启动topology的配置信息对象
        Config config=new Config();
//配置是否生成运行日志，在本地模式中一般用于调试，设为true，会记录下每个组件所发射的每条消息，生产中一般设为flase,减少性能消耗
        config.setDebug(true);
//调用两种运行模式，集群模式和本地模式
        if(args!=null&&args.length>0){
//设置启动的Worker数量
            config.setNumWorkers(3);
//提交任务
          StormSubmitter.submitTopologyWithProgressBar(args[0],config,topologyBuilder.createTopology());
        }
        else{
     config.setMaxTaskParallelism(3);
 new LocalCluster().submitTopology("my-application",config,topologyBuilder.createTopology());
        }



    }
}
