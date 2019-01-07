package spark_core
class Animal{}
class Dog extends Animal{}
class Consumer[T](a:T){}
class Consumer1[+T](a:T){}
class Consumer2[-T](a:T){}
object 协变 {
  def main(args: Array[String]): Unit = {
      val animal = new Animal()
    // 因为T限定为不变类型
    val c = new Consumer[Dog](new Dog)
    var c2:Consumer[Dog]=c

    //协变  父-》子   参数必须都是所用范型类型的本身或者子类
     val unit3 = new Consumer1[Dog](new Dog)
     val unit4:Consumer1[Animal]=unit3
    //因为Consumer定义成协变类型的，所以Consumer[Dog]是Consumer[Animal]的子类型，
    // 所以它可以被赋值给unit4

    //逆变  子-》父
    val d=new Consumer2[Animal](new Dog)
    val d1:Consumer2[Dog]=d

var l=List(1,2,3,4)
    var l1=Array(1,2,3,4)


  }
}
