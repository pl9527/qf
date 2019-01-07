package spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



object Test01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("dsa").setMaster("local")
    val sc = new SparkContext(conf)
    println("百位------------------------------------")

    val rdd: RDD[String] = sc.textFile("E://2.txt",5)
    val rdd1: RDD[(Char, Int)] = rdd.map(f => {
      val a = f.split(" ")(1)
      val chars: Array[Char] = a.toCharArray()
      (chars(2), 1)
    })
    val value= rdd1.reduceByKey(_+_).sortBy(f=>f._2)
    //val rd: RDD[(Int, (Char, Int))] = value.mapPartitionsWithIndex((x,y)=>y.map((x,_)))
    value.foreach(println)
     println("重庆十位------------------------------------")
      val rdd2: RDD[(Char, Int)] = rdd.map(f => {
        val a = f.split(" ")(1)
        val chars: Array[Char] = a.toCharArray()
        (chars(3), 1)
      })
      val value1: RDD[(Char, Int)] = rdd1.reduceByKey(_+_).sortBy(_._2)
      value1.foreach(println)
    println("重庆个位------------------------------------")
      val rdd3: RDD[(Char, Int)] = rdd.map(f => {
        val a = f.split(" ")(1)
        val chars: Array[Char] = a.toCharArray()
        (chars(4), 1)
      })
    rdd3.countByKey()
      val value2: RDD[(Char, Int)] = rdd3.reduceByKey(_+_).sortBy(_._2)
      value2.foreach(println)
  }
}
