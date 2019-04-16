object ZipTest {
  def main(args: Array[String]): Unit = {

    val map1 = Map("guangz"->10,"shenz"->20)

    val map2 = Map("guangz"->5,"shenz"->6)

    val functionToStringToInt: ((Map[String, Int], (String, Int)) => Map[String, Int]) => Map[String, Int] = map1.foldLeft(map2)
  }



}
