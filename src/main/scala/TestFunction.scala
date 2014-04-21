/**
 * Created by liweichao on 14-4-19.
 */
object TestFunction extends App{
  val ls = List(-1, 0, 1,2,3)
  val result = ls flatMap {
    case l:Int if l > 0 => Some(l)
    case _ => None
  }

  val res1 = ls flatMap {
    dd => dd match {
      case l:Int if l > 0 => Some(l)
      case _ => None
    }
  }
  println(s"result = ${result.toString}")
  println(s"res1 = ${res1.toString}")
}
