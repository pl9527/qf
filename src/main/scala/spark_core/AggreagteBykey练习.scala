package spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggreagteBykey练习 extends App {
   val cong=new SparkConf().setAppName("aggre").setMaster("local")
   val sc=new SparkContext(cong)
  // val rdd1: RDD[String] = sc.textFile("E://1.txt",3)
  val rdd = sc.parallelize(1 to 10).sample(false,0.9,3).collect()
while (true){}
  println(rdd.foreach(f=>println(f)))
  sc.stop()


  /* val rdd2=rdd1.flatMap(_.split(" "))
   rdd2.sample(false,0.2,100).foreach(f=>println(f))*/

  /* val rdd3=rdd1.mapPartitions(_).foreachPartition(f=>{
     val list=List()
     while (f.hasNext){
     list::f.next()::Nil
     }
     list.iterator
   })
*/

/* val value: RDD[(String, Int)] = rdd2.map((_,1))

        value.distinct()
  println(value.partitions.size)
  //先combiner
  val rdd4=(a:Int,b:Int)=>a+b
  val rdd5=(a:Int,b:Int)=>a+b
 val value1: RDD[(String, Int)] = value.aggregateByKey(0)(rdd4,rdd5)
value1.foreach(f=>println(f))*/
/*  val a:Any=4
  val b:Any=5
  val c=a+b*/






}
