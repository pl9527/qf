package spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
//
//
// 求班级大于80分的平均成绩
class SUM extends AccumulatorV2[Int,ListBuffer[Int]] {
  val a=new ListBuffer[Int]
  override def isZero: Boolean = a.isEmpty

  override def copy(): AccumulatorV2[Int, ListBuffer[Int]] = {
    val b=new SUM
    b.synchronized{
      b.a.appendAll(a)
    }
    b
  }

  override def reset(): Unit = a.clear()

  override def add(v: Int): Unit = a.append(v)

  override def merge(other: AccumulatorV2[Int, ListBuffer[Int]]): Unit = a.appendAll(other.value)

  override def value: ListBuffer[Int] = a
}

object 累加 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("l").setMaster("local")
    val context = new SparkContext(conf)
    val unit: RDD[(String, Int)] = context.parallelize(Array(("h",80),("g",45),("j",62),("k",100),("l",90)),3)
    val a=new SUM

    context.register(a,"ll")
    unit.map(f => {
      if (f._2 > 80) {
        a.add(f._2)
      }
      (f._1, 1)
    }).collect()

      val iterator: Iterator[Int] = a.value.iterator

    val b=iterator.sum.toDouble

    val iterator1: Iterator[Int] = a.value.iterator

    val a1=iterator1.toList.size
    println(a1)
    println(b/a1)

  }
}
