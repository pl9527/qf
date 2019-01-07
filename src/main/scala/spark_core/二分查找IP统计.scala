package spark_core

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
object 二分查找IP统计  extends App {
  val conf: SparkConf = new SparkConf().setAppName("IPsearch2").setMaster("local")
  val context = new SparkContext(conf)
  val value: RDD[String] = context.textFile("d://ipsearch/ip.txt")

  val Allip: Array[(String, String, String)] = value.map(f => {
    var ipStart = f.split("\\|")(2)
    var ipEnd = f.split("\\|")(3)
    var city = f.split("\\|")(6)
    (ipStart, ipEnd, city)
  }).collect()


  var ip: RDD[String] = context.textFile("d://ipsearch/http.log")
  var result: RDD[(String, Int)] = ip.map(f => {
    var longip: Long = IPtoLong(f.split("\\|")(1))
    var str: String = IPsearch(Allip, longip)
    (str, 1)
  }).reduceByKey(_+_)

  result.foreach(f=>println(f))

  def IPtoLong(ip:String):Long={
    var strings: Array[String] = ip.split("\\.")
    strings(0).toLong<<24+strings(1).toLong<<16+strings(2).toLong<<8+strings(3).toLong
  }

  def IPsearch(array:Array[(String,String,String)],ip:Long):String= {
    var start = 0
    var end = array.length - 1
    var mid =0
    while (start <= end) {
      mid=(end + start) / 2
      if (ip > array(mid)._1.toLong&&ip<array(mid)._2.toLong) {
        return array(mid)._3

      }
      else if (ip < array(mid)._1.toLong) {
        end = mid - 1
      }
      else {
        start=mid+1
      }
    }
    return array(mid)._3
  }

context.stop()
}






