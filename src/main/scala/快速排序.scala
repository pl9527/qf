object 快速排序 {
  def main(args: Array[String]): Unit = {
    val a=Array(200,5,100,80,201,30)
    //取前后下标传入
    val start=0
    val end=a.length-1
    sort(a,start,end )
    for (elem <- a) {
      print(elem+",")
    }
  }
  def sort(arr:Array[Int],low:Int,high:Int): Unit ={
     var start=low
    var end=high
    var num=arr(low)
    while(end>start) {
      //从后往前比较,和数组中第一位比较
      while (end > start && arr(end) >= num) {
        //下标-1，直到有比第一位小的，就交换位置。然后又从前往后比较
        end -= 1
        if (arr(end) <= num) {
          var temp = arr(end)
          arr(end) = arr(start)
          arr(start) = temp
        }
      }

        //从前往后比较
        while (end > start && arr(start) <= num) {
          //下标+1，直到有比第一位大的，就交换位置，
          start += 1
          if (arr(start) >= num) {
            var temp = arr(start)
            arr(start) = arr(end)
            arr(end) = temp
          }
        }
      }

    //此时第一次循环比较结束，关键值的位置确定了，左边的值都比关键值小，右边的都比
    //关键值大，现在两边顺序并没有排好，进行下面递归调用
    if(start>low)
      sort(arr,low,start-1)//左边序列。第一个索引位置到关键值索引-1
    if(end<high)
      sort(arr,end+1,high)//右边序列，第一个索引位置到关键值索引-1

  }
}
