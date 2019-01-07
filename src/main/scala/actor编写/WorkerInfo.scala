package actor编写

case  class WorkerInfo(val id:String,val host:String) {
  var lastHeartBeatTime=System.currentTimeMillis()

  override def toString: String = s"${id+":"+host}"

}
