import akka.actor.ActorSystem
import akka.persistence.{PersistenceSettings, PersistentRepr}
import akka.persistence.hbase.journal.{HBaseClientFactory, PluginPersistenceSettings}
import akka.persistence.hbase.common.Const._
import akka.persistence.hbase.common.Columns._
import akka.persistence.hbase.common._
import akka.persistence.serialization.Snapshot
import com.typesafe.config._
import com.coinport.coinex.serializers._
import java.util.{ArrayList => JArrayList}
import java.io.{Closeable, OutputStreamWriter, BufferedWriter, BufferedInputStream}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.KeyValue
import scala.collection.mutable
import scala.concurrent.Future
import scala.collection.JavaConverters._

import DeferredConversions._

object DumpEventSource extends App {
  override def main(args: Array[String]) {
    if (args.length < 1) {
      sys.error("Enter processorId to dump")
      sys.exit(-1)
    }
    val dumper = new DoDumpEventSource
    dumper.exportData(args(0))
  }
}

class DoDumpEventSource() {
  private val config = ConfigFactory.load(getClass.getClassLoader, "application.conf")
  private implicit val system = ActorSystem("test", config)
  private lazy val fs: FileSystem = openHdfsSystem(config.getString("dump.hdfsHost"))
  private val exportSnapshotHdfsDir = config.getString("dump.dumpSnapshotHdfsDir")
  private val exportMessagesHdfsDir = config.getString("dump.dumpMessagesHdfsDir")
  private val snapshotHdfsDir: String = config.getString("dump.snapshot-dir")
  private val messagesTable = config.getString("dump.table")
  private val messagesFamily = config.getString("dump.family")
  private val cryptKey = config.getString("dump.encryption-settings")
  private val BUFFER_SIZE = 2048
  private val SCAN_MAX_NUM_ROWS = 5
  private val ReplayGapRetry = 5

  implicit var pluginPersistenceSettings = PluginPersistenceSettings(config, JOURNAL_CONFIG)
  implicit var executionContext = system.dispatcher
  implicit var serialization = EncryptingSerializationExtension(system, cryptKey)
  val StartSeqNum: Int = 1

  def exportData(processorId: String) = {
    val toSeqNum = dumpSnapshot(processorId, StartSeqNum)
    println("*" * 50 + s" toSeqNum : ${toSeqNum}")
    dumpMessages(processorId, StartSeqNum, Long.MaxValue)
  }

  def dumpSnapshot(processorId: String, processedSeqNum: Long): Long = {
    val snapshotMetas = listSnapshots(snapshotHdfsDir, processorId)
    if (snapshotMetas.isEmpty) //no file to process, let processedSeqNum to former process's lastNum, which is processedSeqNum - 1
      return processedSeqNum - 1
    snapshotMetas.head match {
      // when lastSeqNum == processedSeqNum, there is one message
      case desc@HdfsSnapshotDescriptor(processorId: String, seqNum: Long, _) if (seqNum >= processedSeqNum) =>
        val path = new Path(snapshotHdfsDir, desc.toFilename)
        val snapshot =
          serialization.deserialize(
            withStream(new BufferedInputStream(fs.open(path, BUFFER_SIZE), BUFFER_SIZE)) {
              IOUtils.toByteArray
            }, classOf[Snapshot])
        val className = snapshot.data.getClass.getEnclosingClass.getSimpleName
        writeSnapshot(exportSnapshotHdfsDir, processorId, seqNum, snapshot, className)
        seqNum
      case _ => processedSeqNum - 1
    }
  }

  // "fromSeqNum" is inclusive, "toSeqNum" is exclusive
  def dumpMessages(processorId: String, fromSeqNum: Long, toSeqNum: Long): Future[Unit] = {
    if (toSeqNum <= fromSeqNum) return Future(())
    val client = getHBaseClient()
    var retryTimes: Int = 0
    var isDuplicate = false
    var tryStartSeqNr: Long = if (fromSeqNum <= 0) 1 else fromSeqNum

    var scanner: SaltedScanner = null
    type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

    def hasSequenceGap(columns: collection.mutable.Buffer[KeyValue]): Boolean = {
      val processingSeqNr = sequenceNr(columns)
      if (tryStartSeqNr != processingSeqNr) {
        if (tryStartSeqNr > processingSeqNr) {
          sys.error(s"Replay $processorId Meet duplicated message: to process is $tryStartSeqNr, actual is $processingSeqNr")
          isDuplicate = true
        }
        return true
      } else {
        return false
      }
    }

    def initScanner() {
      if (scanner != null) scanner.close()
      scanner = new SaltedScanner(client, pluginPersistenceSettings.partitionCount, Bytes.toBytes(messagesTable), Bytes.toBytes(messagesFamily))
      scanner.setSaltedStartKeys(processorId, tryStartSeqNr)
      scanner.setSaltedStopKeys(processorId, RowKey.toSequenceNr(toSeqNum))
      scanner.setKeyRegexp(processorId)
      scanner.setMaxNumRows(SCAN_MAX_NUM_ROWS)
    }

    def sequenceNr(columns: mutable.Buffer[KeyValue]): Long = {
      for (column <- columns) {
        if (java.util.Arrays.equals(column.qualifier, SequenceNr)) {
          return Bytes.toLong(column.value())
        }
      }
      0L
    }

    // (isFailed, failedErrMsg, writeMsg)
    def getMessages(rows: AsyncBaseRows): (Boolean, String, String) = {
      val builder = new StringBuilder()
      for (row <- rows.asScala) {
        if (hasSequenceGap(row.asScala) && retryTimes < ReplayGapRetry) {
          if (isDuplicate) {
            return (true, "Duplicated message", builder.toString())
          }
          sys.error(s"Meet gap at ${tryStartSeqNr}")
          retryTimes += 1
          Thread.sleep(100)
          initScanner()
          return (false, "", builder.toString())
        } else {
          if (retryTimes >= ReplayGapRetry) {
            return (true, s"Gap retry times reach ${ReplayGapRetry}", builder.toString())
          }
          builder ++= "{"
          for (column <- row.asScala) {
            if (java.util.Arrays.equals(column.qualifier, Message) || java.util.Arrays.equals(column.qualifier, SequenceNr)) {
              if (java.util.Arrays.equals(column.qualifier, Message)) {
                // will throw an exception if failed
                val msg = serialization.deserialize(column.value(), classOf[PersistentRepr])
                builder ++= "\"" ++= msg.payload.getClass.getEnclosingClass.getSimpleName ++= "\":"
                builder ++= PrettyJsonSerializer.toJson(msg.payload)
              } else {
                builder ++= "\"" ++= Bytes.toString(column.qualifier) ++= "\":"
                builder ++= Bytes.toLong(column.value()).toString
                tryStartSeqNr = Bytes.toLong(column.value()) + 1
              }
              builder ++= ","
            }
          }
          builder.delete(builder.length - 1, builder.length)
          builder ++= "},"
          retryTimes = 0
        }
      }
      (false, "", builder.toString())
    }

    def handleRows(): Future[Unit] = {
      scanner.nextRows() map {
        case null =>
          scanner.close()
          Future(())
        case rows: AsyncBaseRows =>
          val (isFailed, errMsg, writeMsg) = getMessages(rows)
          if (!writeMsg.isEmpty && tryStartSeqNr > 0) {
            writeMessages(writeMsg, tryStartSeqNr - 1)
          }
          if (isFailed) {
            sys.error(errMsg)
            Future.failed(new Exception(errMsg))
          } else {
            handleRows()
          }
      }
    }

    def writeMessages(data: String, seqNum: Long) {
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(
        new Path(exportMessagesHdfsDir, s"coinport_events_${processorId}_${String.valueOf(seqNum).reverse.padTo(16, "0").reverse.mkString}_v1.json".toLowerCase))))
      writer.write( s"""{"timestamp": ${System.currentTimeMillis()},\n"events": [""")
      writer.write(data.substring(0, data.length - 1))
      writer.write("]}")
      writer.flush()
      writer.close()
    }

    initScanner
    handleRows()
  }

  def writeSnapshot(outputDir: String, processorId: String, seqNum: Long, snapshot: Snapshot, className: String) {
    val json = PrettyJsonSerializer.toJson(snapshot.data)
    val jsonSnapshot = s"""{"timestamp": ${System.currentTimeMillis()},\n"${className}": ${json}}"""
    val exportSnapshotPath = new Path(outputDir,
      s"coinport_snapshot_${processorId}_${String.valueOf(seqNum).reverse.padTo(16, "0").reverse.mkString}_v1.json".toLowerCase)
    withStream(new BufferedWriter(new OutputStreamWriter(fs.create(exportSnapshotPath, true)), BUFFER_SIZE))(IOUtils.write(jsonSnapshot, _))
  }

  private def getHBaseClient() = {
    val hBasePersistenceSettings = PluginPersistenceSettings(config, "hbase-journal")
    HBaseClientFactory.getClient(hBasePersistenceSettings, new PersistenceSettings(config.getConfig("akka.persistence")))
  }

  private def openHdfsSystem(defaultName: String): FileSystem = {
    val conf = new Configuration()
    conf.set("fs.default.name", defaultName)
    FileSystem.get(conf)
  }

  private def withStream[S <: Closeable, A](stream: S)(fun: S => A): A =
    try fun(stream) finally stream.close()

  private def listSnapshots(snapshotDir: String, processorId: String): Seq[HdfsSnapshotDescriptor] = {
    val descs = fs.listStatus(new Path(snapshotDir)) flatMap {
      HdfsSnapshotDescriptor.from(_, processorId)
    }
    if (descs.isEmpty) Nil else descs.sortWith(_.seqNumber > _.seqNumber).toSeq
  }

}