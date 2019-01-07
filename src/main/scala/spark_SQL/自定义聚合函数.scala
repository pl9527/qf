package spark_SQL

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql._

import scala.Double._
case class People(age:Int,name:String)

class Myudaf extends UserDefinedAggregateFunction{
  //你使用这个函数需要传进来的数据类型  只对应表里面输入进来的数据类型,用input来获取输入值
  override def inputSchema: StructType = new StructType().add("age",LongType).add("j",LongType)
  //中间缓存的数据的类型，根据你的业务来,看需要的是什么和什么类型
  override def bufferSchema: StructType =new StructType().add("sum",LongType).add("count",LongType)
  //最终输出的结果的数据类型
  override def dataType: DataType =DoubleType
  // 函数的稳定性  给同一个输入，输出是否相同
  override def deterministic: Boolean = true
 //你这个缓存中间的初始化,你缓存有几个东西就要初始化几个东西,由buffer(下标)来代表
  override def initialize(buffer: MutableAggregationBuffer): Unit ={
    buffer(0)=0L
    buffer(1)=0L
  }
//更新Executor端的缓存数据,这个input是从表里面来的
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)) { // 防止表里面为NULL的数据

      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }
//合并多个Executor端的缓存数据,由buffer1(下标)来代表
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }
//将缓存进行最终聚合,任意写逻辑 并返回  0号位的缓存 与1号位的缓存
  override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble/buffer.getLong(1)
}



case class Avg(var sum:Long,var count:Long)
object Myudaf11 extends Aggregator [People,Avg,Double]{

 //初始化缓存
  override def zero: Avg = Avg(0L,0L)
//Executor内聚合，更新缓存内容//指定表输入的数据
  override def reduce(b: Avg, a: People): Avg ={b.sum+=a.age; b.count+=1 ; b}
//Executor间缓存聚合
  override def merge(b1: Avg, b2: Avg): Avg = {b1.sum+=b2.sum;b1.count+=b2.count;b1}
//最后输出的缓存结果
  override def finish(reduction: Avg): Double ={reduction.sum.toDouble/reduction.count}
//转换编码器，强类型所需要
  override def bufferEncoder: Encoder[Avg] = Encoders.product
//设定输出值的编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
object 自定义聚合函数 {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .master("local[*]").appName("sql").getOrCreate()

     //导入隐式转换
      import session.implicits._
        //自定义UDF函数
    session.udf.register("da",(x:String)=>"name:"+x)
    session.udf.register("udaf_avg",new Myudaf)

    //构建一个dataFrame
   val unit: Dataset[String] = session.read.textFile("E:\\1.txt")
    val d=  unit.map(x=>{val r=x.split(" ")
      People(r(0).toInt,r(1))
    })
    //使用用户自定义聚合函数（UDAF）
    //可以转换为dataset
    val ds: Dataset[People] = d.as[People]
    //ds注册一张表
    ds.createTempView("dspeople1")
    //toColumn 表示操作整个数据集
        val value: TypedColumn[People, Double] = Myudaf11.toColumn.name("age")

   //df注册一张表
   d.createTempView("people")

 val frame: DataFrame = session.sql("select udaf_avg(age),name from people group by name")

//将结果保存到hdfs
//frame.write.mode("append").format("jdbc")....


  }
}
