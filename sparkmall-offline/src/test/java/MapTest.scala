import scala.collection.immutable

object MapTest {
  def main(args: Array[String]): Unit = {
    val map1 = Map("a"->1,"b"->2,"c"->3)
    val map2 = Map("a"->10,"d"->3)

    val map3: Map[String, Int] = map1 ++ map2.map {
      case (k, v) => (k -> (map1.getOrElse(k, 0) + v))
    }
    println(map3)
  }
}
