package spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object 基站停留时间练习 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("local").setMaster("local")
    val context = new SparkContext(conf)
val rdd1: RDD[String] = context.textFile("E://log/")
    //将日志中数据解析出来，注意0是离开，1是停留 就这样搞
    val rdd2:RDD[((String,String),Long)]=rdd1.map(f=>{
     val user: String = f.split(",")(0)
      var TIME: Long = f.split(",")(1).toLong
       val jizhan: String = f.split(",")(2)
      var TiME1=0L
      if(f.split(",")(3).toInt==0){
        TiME1= TIME
      }
      else{
        TiME1= -TIME
      }
      ((user,jizhan),TiME1)
    })



val rdd3: RDD[((String, String), Long)] = rdd2.reduceByKey(_+_)

    //统计完成后需要把信息整合，等下要join基站经纬度信息
   val rdd4:RDD[(String,(String,Long))]=rdd3.map(f=>{
      (f._1._2,(f._1._1,f._2))
    })


    //然后把基站的经纬度那些信息给解析出来，等下要join用户信息
     val a: RDD[String] = context.textFile("E://lac_info.txt")
    val a1=a.map(f=>{
      (f.split(",")(0),(f.split("1")(1),f.split(",")(2)))

    })

    //进行连接,获取结果
    val unit: RDD[(String, ((String, String), (String, Long)))] = a1.join(rdd4)
    val unit1: RDD[(String, List[(String, ((String, String), (String, Long)))])] = unit.groupBy(_._2._2._1).mapValues(_.toList.sortBy(_._2._2._2))
unit1.foreach(println)


  }
}
