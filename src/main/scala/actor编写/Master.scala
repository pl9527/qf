package actor编写


import scala.concurrent.duration._
import akka.actor.{Props, ActorSystem, Actor}
import scala.concurrent._
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.mutable
/**
  * Master Actor
  */
class Master extends Actor{
  //用于保存所有Worker的所有信息
  var workers=scala.collection.mutable.HashMap[String,WorkerInfo]()
  val workers1= new mutable.HashSet[WorkerInfo]
 var WORK_TIMEOUT=10*1000
  override def preStart(): Unit = {
    //启动一个定时器，间隔时间去检测是否有Worker offline
    context.system.scheduler.schedule(5 seconds,WORK_TIMEOUT millis,self,CheckOfflineMessage)
  }
  override def receive: Receive = {
    // 处理离线检测消息
case CheckOfflineMessage=>{
  println("current workers:"+workers.size)
  val currentTime = System.currentTimeMillis()
  val toRemove = workers1.filter(w => currentTime-w.lastHeartBeatTime>WORK_TIMEOUT).toArray
  for(worker<-toRemove){
    workers1-= worker
    workers.remove(worker.id)
  }

}
  //处理Worker的注册消息
case RegisterMessage(id,host)=>{
  println(s"${"register worker:"+id+host}")
  if(workers.contains(id)){
val woker=WorkerInfo(id,host)
    workers(id)=woker
    //表明worker已经注册完成，回复ack信息
    sender !Ackmessage()
  }
}
case HeartBeatMessage(id)=>{
  println(s"${"receive worker heartbeat:"+id}")
val worker=workers(id)
  worker.lastHeartBeatTime=System.currentTimeMillis()
}
  }
    //用于Actor构造完后第一次执行，只执行一次

}
object Master extends App{
  val host="localhost"
  val port=9999
  val configStr=
     s"""
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.hostname = "$host"
      |akka.remote.netty.tcp.port = "$port"
    """.stripMargin



  val config=ConfigFactory.parseString(configStr)
  val actorSystem=ActorSystem.create("MasterAs",config)
  actorSystem.actorOf(Props[Master],"Master")
}
