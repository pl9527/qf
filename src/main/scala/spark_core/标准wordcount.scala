package spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



  object 标准wordcount {

    def main(args: Array[String]): Unit = {


      //1.创建spark的配置信息
      val sparkConf: SparkConf = new SparkConf().setAppName("WordCountTopology")

      //2.初始化与spark的连接 sc
      val sc = new SparkContext(sparkConf)

      //3.读取文件
      val line: RDD[String] = sc.textFile(args(0))

      //4.切分每一行
      val word: RDD[String] = line.flatMap(_.split(" "))

      //5.将没一个单词和1 映射成二元组
      val wordToOne: RDD[(String, Int)] = word.map((_,1))

      //6.调用reduceByKey进行聚合
      val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

      //7.将结果数据保存到文件中
      wordToCount.saveAsTextFile(args(1))

      //8.关闭连 接
      sc.stop()

    }




}
