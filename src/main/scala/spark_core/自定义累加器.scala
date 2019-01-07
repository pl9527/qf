package spark_core

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

class MyACCU1 extends AccumulatorV2[String,java.util.Set[String]]{
val logset=new util.HashSet[String]
//判断累加器它是否为空
  override def isZero: Boolean =logset.isEmpty
//将数据复制到一个新的累加器里面并返回该对象
    override def copy(): AccumulatorV2[String, util.Set[String]] = {
      val acc=new MyACCU1
    acc.synchronized{
      acc.logset.addAll(logset)
    }
    acc
  }
//重置该累加器
  override def reset(): Unit = logset.clear()
//进行值的累加
  override def add(v: String): Unit = logset.add(v)
//将传过来的累加器中的累加数据复制到此累加器
  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    logset.addAll(other.value)
  }
//返回累加器的value值
  override def value: util.Set[String] = logset
}
object 自定义累加器 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("JJ").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val value: RDD[String] = sc.textFile("E:\\1.txt")
    val u = new MyACCU1
     sc.register(u,"自定义单词统计器")
    value.map(x=>{
      if(x.contains("hadoop")){
              u.add(x)
      }
      x
    }).collect()//触发计算
//开始计算累加器的值
        val unit: util.Iterator[String] = u.value.iterator()
  while(unit.hasNext){
    println(unit.next())
  }
    sc.stop()
  }
}