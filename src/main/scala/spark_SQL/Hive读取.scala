package spark_SQL

import org.apache.spark.sql.{DataFrame, SparkSession}

object Hive读取 {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("s")
      .master("local")
      .config("spark.sql.warehouse.dir", "E://SPARK1_OUTPUT/")//
      .enableHiveSupport()
      .getOrCreate()

    val frame: DataFrame = session.sql("show databases")
    //session.sql("create database uuu")
    session.sql("select * from u1")
    frame.show()
session.stop()
  }
}
