package spark_core

import java.io


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RDD读取HBase数据  extends  App {
  /*val conf = new SparkConf().setAppName("app1").setMaster("local[*]")
  val context = new SparkContext(conf)
  //创建Hbase配置信息
  val conf1 = HBaseConfiguration.create()
  conf1.set("hbase.zookeeper.quorum", "SZ01,SZ03,SZ04")
  conf1.set(TableInputFormat.INPUT_TABLE, "day")

  //形成RDD
  val value: RDD[(ImmutableBytesWritable, Result)] = context.newAPIHadoopRDD(conf1, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
  //
  val count = value.count()
  println(count)
  //对HbaseRDD进行处理
  value.foreach(x => {
    val result = x._2
    //拿到result
    val cells: Array[Cell] = result.rawCells()
    for (cell <- cells) {

      print(Bytes.toString(CellUtil.cloneRow(cell))+" ")
      print(Bytes.toString(CellUtil.cloneFamily(cell))+" ")
      print(Bytes.toString(CellUtil.cloneQualifier(cell))+" ")
      print(Bytes.toString(CellUtil.cloneValue(cell))+" ")
    }
  })
context.stop()*/

}

