package spark_core

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JDbcRDD extends App{

 val conf: SparkConf = new SparkConf().setAppName("Mysql").setMaster("local[*]")
 val context = new SparkContext(conf)
 //定义连接mysql的参数
  val driver="com.mysql.jdbc.Driver"
  val url="jdbc:mysql://localhost:3306/pp"
  val userName="root"
  val password="root"
  val jdbc=new JdbcRDD(context,()=>{
    Class.forName(driver)
    DriverManager.getConnection(url,userName,password)
      },"select * from orders where id=? and purchaserId=? ",1,6,3,x=>{
  x.getInt(1)
}

  )
               jdbc.foreach(println)







}
