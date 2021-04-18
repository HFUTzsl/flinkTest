/**
  * @Author shanlin
  * @Date Created in  2021/1/21 21:38
  *
  */
object tst {
  def main(args: Array[String]): Unit = {
    val list: List[Any] = List(1,"aaa",0.9)
    val tuple: (String, Int) = ("wqq",1)
    for (i <- list){
      println(i)
    }

  }
}
