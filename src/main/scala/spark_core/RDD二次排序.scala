package spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
case class Girl(f:Int,y:Int) extends Ordered[Girl]{
  override def compare(that: Girl): Int = {
    if(this.f==that.f){
      this.y-that.y
    }
    else{
      that.f-this.f
    }


  }
}
object RDD二次排序 {
  def main(args: Array[String]): Unit = {
  val conf=new SparkConf().setAppName("dd").setMaster("local")
 val context = new SparkContext(conf)
    val girl=context.parallelize(Array((88,22),(88,33),(40,33),(40,22)))
     val unit: RDD[(Int, Int)] = girl.sortBy(f=>new Girl(f._1,f._2),false)
    println(unit.collect().toBuffer)

  }

}
