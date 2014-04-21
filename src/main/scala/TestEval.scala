import com.twitter.util.Eval
import java.io.InputStream
import org.apache.commons.io.IOUtils
import scala.collection.mutable.Map

object TestEval extends App {
  override def main(args: Array[String]) {
    val map = loadMap("map_conf.scala")
    println("result = " + map.toString())
  }

  protected def loadMap(mapConfig: String): Map[String, Long] = {
    val in: InputStream = this.getClass.getClassLoader.getResourceAsStream(mapConfig)
    (new Eval()(IOUtils.toString(in))).asInstanceOf[Map[String, Long]]
  }
}
