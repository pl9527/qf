package spark_SQL

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}

object DataFrame读取MYSQL {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("mYsql").getOrCreate()
    import session.implicits._
    val reader:DataFrame = session.read.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/project")
      .option("user","root")
      .option("password","123456")
      .option("dbtable","aa").load()
     reader.show(2)
  }
}
