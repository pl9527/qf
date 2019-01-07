package spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val ss: SparkConf = new SparkConf().setMaster("local").setAppName("ss")
    val context = new SparkContext(ss)
    val unit: RDD[String] = context.textFile("e://1.txt")
    unit.cache()

    val unit1: RDD[Int] = unit.map(f => {
      var f1 = f.split(" ")
      f1.length
    })
    println(unit1.max())
    unit.count()
    unit.take(3)
  }
}
