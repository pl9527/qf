package spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//类似map()算子，区别就是，map()算子一次处理一条数据，但是mapP..一次处理一个parition中的所有数据
//当rdd中的pairition中的数据量不是很大，那么建议使用mapParition(),
// 这样虽然会加快处理速度，如果rdd中的数据很大，比如10亿条，可能会导致oom
object MapParition练习 extends App{
  var conf=new SparkConf().setAppName("DS").setMaster("local")
  val sc=new SparkContext(conf)

  val rdd1:RDD[Int]=sc.parallelize(1 to 10000,4)
val rdd2:RDD[Int]=rdd1.mapPartitions(f=> {
  var list = List[Int]()
  var sum = 0
  while (f.hasNext) {
    sum +=f.next()
  }
  list.::(sum).iterator
}
)
  rdd2.foreach(f=>println(f))

}

