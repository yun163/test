import scala.concurrent.{Promise, ExecutionContext, Future}
import org.hbase.async._
import com.stumbleupon.async.{Callback, Deferred}
import concurrent.ExecutionContext.Implicits.global

//implicit def typedFuture2unitFuture[T](f: Future[T])(implicit executionContext: ExecutionContext): Future[Unit] =
//  f map { _ => () }
//

//implicit def futureToFutureUnit(normal :Future[AnyRef]): Future[Unit] = {
//  new Future[Unit]()
//}

case class TestAsyncHBase(zookeeper : String, tableName: String, family: String = "d") {
  val client: HBaseClient = new HBaseClient(zookeeper)
  client.setFlushInterval(1)
  val value = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

  implicit def typedFuture2unitFuture[T](f: Future[T])(implicit executionContext: ExecutionContext): Future[Unit] =
    f map { _ => () }

  implicit def deferred2unitFuture[T <: AnyRef](deferred: Deferred[AnyRef])(implicit executionContext: ExecutionContext): Future[Unit] =
    deferred2future(deferred)

  implicit def deferred2future[T <: AnyRef](deferred: Deferred[T]): Future[T] = {
    val p = Promise[T]()

    val onSuccess = new Callback[AnyRef, T] {
      def call(in: T) = p.success(in)
    }

    val onError = new Callback[Any, Exception] {
      def call(ex: Exception) = p.failure(ex)
    }

    deferred
      .addCallback(onSuccess)
      .addErrback(onError)

    p.future
  }

  protected def executePut(key: Array[Byte], qualifiers: Array[Array[Byte]], values: Array[Array[Byte]]): Future[Unit] = {
    val request = new PutRequest(tableName.getBytes(), key, family.getBytes(), qualifiers, values)
    client.put(request)
  }

  protected def onePut(id :Long) {
    val futures:Seq[Future[Unit]] = (id until (id + 1)) map { i =>
      executePut((System.currentTimeMillis() * 1000000 + i).toString.getBytes(), Array[Array[Byte]]("cd".getBytes), Array[Array[Byte]](value.getBytes))
    }
    Future.sequence(futures) map {
      case _ => client.flush()
    }
  }

  protected def batchPut() {
    (0 until 1000000) map {
      i => onePut(i)
    }
  }

  protected def shutdown() {
    client.shutdown()
  }
}

object TestAsyncHBase extends App {
  val tester = TestAsyncHBase("localhost", "testAsync")
  val start = System.currentTimeMillis()
  tester.batchPut()
  tester.shutdown()
  println (s"""asyncHbase cost ${System.currentTimeMillis() - start} ms""")
}
