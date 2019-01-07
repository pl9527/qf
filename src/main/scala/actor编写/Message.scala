package actor编写
//消息类，表示Workers的注册消息
case class RegisterMessage(val id:String,val host:String) {

}
//注册发送过去的回复
case class Ackmessage()
//消息类，表示worker的心跳消息
case class HeartBeatMessage(val id:String)
// 消息类，表示Maser自己向自己发送的检查Worker是否失效的信息，去检测它

case  class CheckOfflineMessage()
//消息类，表示Worker向自己发送的执行心跳的消息
case  class SendheartBeat()



