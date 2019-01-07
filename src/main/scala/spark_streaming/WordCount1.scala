package spark_streaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object WordCount1 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("SD").setMaster("local[2]")
    val context = new StreamingContext(conf,Seconds(5))
    context.checkpoint("E://output/")//在计算全量数据时需要设置
    val Array(zkQuorum,group,topics,numThreads)=args

//封装为后面创建dsream传入的topic的map
 val map: Map[String, Int] = topics.split(",").map((_,numThreads.toInt)).toMap
//获取kafka的数据。转换为dstream
    val unit:ReceiverInputDStream[(String,String)]=KafkaUtils.createStream(context,zkQuorum,group,map,StorageLevel.MEMORY_AND_DISK)
//获取到的数据是RDD元组（偏移量，获取的value）
    val value: DStream[String] = unit.map(_._2)
    //取到的value进行分析

    /** val res: DStream[(String, Int)] = tups.updateStateByKey(
      func, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
      *
      * Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)]
  // 在调用updateStateByKey中，需要传入一个用于计算历史批次和当前批次数据的函数
  // 该函数中有几个类型：String, Seq[Int], Option[Int])]
  // String代表元组中每一个单词，也就是key
  // Seq[Int]代表当前批次相同key对应的value，比如Seq(1,1,1,1)
  // Option[Int]代表上一批次中相同key对应的累加的结果，有可能有值，有可能没有值。
  // 此时，获取历史批次的数据时，最好用getOrElse方法

      */
  val value1: DStream[(String, Int)] = value.flatMap(_.split(" ")).map((_,1))
    val value2: DStream[(String, Int)] = value1.reduceByKeyAndWindow((x:Int,y:Int)=>x-y,Seconds(10),Seconds(10))


    val func1=(t:Iterator[(String,Seq[Int],Option[Int])])=>t.map{
      case(x,y,z)=>{
        (x,y.sum,z.getOrElse(0))
      }
    }

     value.print()
    context.start()
    //等待优雅的退出
    context.awaitTermination()

  }
}




