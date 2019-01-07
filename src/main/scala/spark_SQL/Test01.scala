package spark_SQL
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{types, _}


/**
  * 查询成绩为80分以上的学生基本信息和成绩信息
  */
case class  KK(name:Int,age:String)
object Test01 {
  def main(args: Array[String]): Unit = {
  val d: SparkConf = new SparkConf().setMaster("local").setAppName("d")
    val context = new SparkContext(d)
    val session: SparkSession = SparkSession.builder().appName("d").master("local").getOrCreate()
    import session.implicits._
    val frame: DataFrame = session.read.json("E://2.txt")
       frame.createTempView("stu_score")
    val frame1: DataFrame = session.sql("select name,score from stu_score where score >80")

    frame1.createOrReplaceTempView("score")
                 frame1.show()

    val frame2: DataFrame = session.read.json("E://3.txt")
    frame2.createTempView("stu")

  val unit = frame1.rdd.map(x=>x(0)).collect()

    var sql="select name,age from stu where name in ("
    for(i<- 0 until unit.length){
      sql=sql+"'"+unit(i)+"'"
      if(i < unit.length-1){
        sql+=","
      }
    }
    sql +=")"
    val frame5: DataFrame = session.sql(sql)
    frame5.createOrReplaceTempView("stu1")

    frame5.show()
    val frame6: DataFrame = session.sql("select count(age) from score left join stu1 on stu1.name=score.name group by age" )

frame6.write.json("E://3.json")
    frame6.show()
















  }
}
