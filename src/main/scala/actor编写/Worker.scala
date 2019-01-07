package actor编写

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.duration._
import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.concurrent.ExecutionContext.Implicits.global


class Worker extends Actor{
  //master actor的引用，就是等下代表选择哪个master
  var master:ActorSelection=null
  val id=UUID.randomUUID().toString
  override def receive: Receive = {
    //注册成功后收到master发来的消息
    case Ackmessage()=>{
      println(s"${"Register AckMessage"}")
      context.system.scheduler.schedule(0 millis,5000 millis,self,SendheartBeat)
    }
      //处理自己给自己发送的心跳消息
    case SendheartBeat=>{
      println("Send heartbeat")
      master!HeartBeatMessage(id)
    }

  }

  override def preStart(): Unit = {
    master=context.system.actorSelection("akka.tcp://MasterAs@localhost:9999/user/Master")
    master !RegisterMessage(id,"localhost")

  }
}
object Worker extends App{
val clientport=8889
  val configStr=
    s"""
       |akka.actor.provider="akka.remote.RemoteActorRefProvider"
       |akka.remote.netty.tcp.port=$clientport
     """.stripMargin
  val config=ConfigFactory.parseString(configStr)
  val actorSystem=ActorSystem("WorkActorSystem",config)
  //启动Actor，Master会被实例化
  actorSystem.actorOf(Props[Worker],"Worker")
}
