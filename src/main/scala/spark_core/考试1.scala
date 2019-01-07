package spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object 考试1 {
  def main(args: Array[String]): Unit = {
    var conf=new SparkConf().setAppName("测试").setMaster("local")
    val sc = new SparkContext(conf)
  val rdd: RDD[String] = sc.textFile("E://2.txt")
    val rdd2:RDD[(String, List[(String, String)])]=rdd.map(f=> {
      var a = f.split("\t")
      (a(0),(a(1),a(2)))
    }
    ).groupByKey().mapValues(f => {
      f.toList.sortBy(_._1).take(2)
    })
rdd2.countByKey()
    val arr=new ArrayBuffer[String]
    rdd2.collect().foreach(f=>{
    f._2.foreach(y=>arr.append(f._1+"\t"+y._1+"\t"+y._2))
    })
    val unit: RDD[String] = sc.parallelize(arr)

    unit.saveAsTextFile("F://3.txt")









  }
}
