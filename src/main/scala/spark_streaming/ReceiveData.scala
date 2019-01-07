package spark_streaming

import java.io.{BufferedReader, FileReader, InputStream, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

                   //监控一个主机               //读入类型，存储级别
class ReceiveData(host:String, port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

                     def receive():Unit = {
                       val socket=new Socket(host,port)
                       val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
                       var str: String = reader.readLine()
                       while(!str.isEmpty){
                         store(str)//把读进来的数据封装传给spark

                         //读取下一行
                        str= reader.readLine()
                       }
                       //如果一段时间我们没读取数据了，就会退出循环
                       //解决方式：关闭资源并重启
                        reader.close()
                       socket.close()
                       restart("没有数据了，重启中")


                     }
                     //启动接收器接收数据
                     override def onStart(): Unit = {
                       //线程接收
                       new Thread("入口一"){
                         override def run(): Unit = {
                           receive()//真正核心方法
                         }
                         }


                     }
                    //资源回收，关闭
                     override def onStop(): Unit = {

                     }
                   }
