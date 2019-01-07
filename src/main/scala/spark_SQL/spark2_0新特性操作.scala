package spark_SQL

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

//需求：统计部门的平均薪资和平均年龄
//条件和思路：
//1、统计出年龄在20岁以上的员工
//2、根据部门名称和员工性别进行分组
//3、开始统计每个部门分性别的平均薪资和平均年龄
case class Empl(Did:BigInt,Dname:String)
object spark2_0新特性操作 {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("s")
      .master("local[4]")
      .getOrCreate()

    import session.implicits._
    //用到某些函数的时候需要导入function包，如AVG
    import org.apache.spark.sql.functions._

   //  val frame: DataFrame = session.read.json("E://4.txt")
     val frame1: DataFrame = session.read.json("E://5.txt")
val value: Dataset[Empl]=frame1.as[Empl]
    println(value.rdd.partitions.length)
//会发生shuffle，分区数多变少，少变多都可以
    val value1: Dataset[Empl] = value.repartition(4)
    println(value1.rdd.partitions.length)
//coalesce只能从多个分区分成少量分区
    val b: Dataset[Empl] = value.coalesce(2)
   println( b.rdd.partitions.length)





session.stop()

  }
}
