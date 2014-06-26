import akka.actor.ActorSystem
import akka.persistence.{PersistenceSettings}
import akka.persistence.hbase.journal.{HBaseClientFactory, PluginPersistenceSettings}
import akka.persistence.hbase.common.Const._
import akka.persistence.hbase.common.Columns._
import akka.persistence.hbase.common._
import com.typesafe.config._
import java.util.{ArrayList => JArrayList}
import java.io._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Bytes._
import org.hbase.async.KeyValue
import scala.collection.mutable
import scala.concurrent.Future
import scala.collection.JavaConverters._

import DeferredConversions._

object MigrateEventSource extends App {
  override def main(args: Array[String]) {
    if (args.length < 2) {
      sys.error("Enter formProcessorId and toProcessorId to migrate")
      sys.exit(-1)
    }
    val migrator = new EventSourceMigrator
    migrator.migrate(args(0), args(1))
  }
}

class EventSourceMigrator extends AsyncBaseUtils {
  private val config = ConfigFactory.load(getClass.getClassLoader, "migrator.conf")
  private implicit val system = ActorSystem("test", config)
  private val messagesTable = config.getString("migrator.table")
  private val messagesFamily = config.getString("migrator.family")

  private val cryptKey = config.getString("migrator.encryption-settings")
  private val output = config.getString("migrator.result")
  private val SCAN_MAX_NUM_ROWS = 5
  private val ReplayGapRetry = 5

  implicit val settings = PluginPersistenceSettings(config, JOURNAL_CONFIG)
  implicit var executionContext = system.dispatcher
  implicit var serialization = EncryptingSerializationExtension(system, cryptKey)
  val StartSeqNum: Int = 1
  val client = getHBaseClient()
  val printerWriter: java.io.PrintWriter = new PrintWriter(new java.io.File(output))

  override def getTable: String = messagesTable
  override def getFamily: String = messagesFamily

  def migrate(fromProcessorId: String, toProcessorId: String) = {
    migrateMessages(fromProcessorId, toProcessorId, StartSeqNum, Long.MaxValue, output)
    printerWriter.close()
  }

  // "fromSeqNum" is inclusive, "toSeqNum" is exclusive
  def migrateMessages(fromProcessorId: String, toProcessorId: String, fromSeqNum: Long, toSeqNum: Long, output: String): Future[Unit] = {
    if (toSeqNum <= fromSeqNum) return Future(())

    var retryTimes: Int = 0
    var isDuplicate = false
    var tryStartSeqNr: Long = if (fromSeqNum <= 0) 1 else fromSeqNum


    var scanner: SaltedScanner = null
    type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

    def hasSequenceGap(columns: collection.mutable.Buffer[KeyValue]): Boolean = {
      val processingSeqNr = sequenceNr(columns)
      if (tryStartSeqNr != processingSeqNr) {
        if (tryStartSeqNr > processingSeqNr) {
          sys.error(s"Replay $fromProcessorId Meet duplicated message: to process is $tryStartSeqNr, actual is $processingSeqNr")
          isDuplicate = true
        }
        return true
      } else {
        return false
      }
    }

    def initScanner() {
      if (scanner != null) scanner.close()
      scanner = new SaltedScanner(client, settings.partitionCount, Bytes.toBytes(messagesTable), Bytes.toBytes(messagesFamily))
      scanner.setSaltedStartKeys(fromProcessorId, tryStartSeqNr)
      scanner.setSaltedStopKeys(fromProcessorId, RowKey.toSequenceNr(toSeqNum))
      scanner.setKeyRegexp(fromProcessorId)
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

    // (isFailed, failedErrMsg)
    def writeMessages(rows: AsyncBaseRows): (Boolean, String) = {
      for (row <- rows.asScala) {
        if (hasSequenceGap(row.asScala) && retryTimes < ReplayGapRetry) {
          if (isDuplicate) {
            return (true, "Duplicated message")
          }
          sys.error(s"Meet gap at ${tryStartSeqNr}")
          retryTimes += 1
          Thread.sleep(100)
          initScanner()
          return (false, "")
        } else {
          if (retryTimes >= ReplayGapRetry) {
            return (true, s"Gap retry times reach ${ReplayGapRetry}")
          }
          var sequenceNr = 0L
          var msg: Array[Byte] = null
          var marker: Array[Byte] = null
          for (column <- row.asScala) {
            if (java.util.Arrays.equals(column.qualifier, SequenceNr)) {
              sequenceNr = Bytes.toLong(column.value())
              tryStartSeqNr = sequenceNr + 1
            } else if (java.util.Arrays.equals(column.qualifier, Message)) {
              msg = column.value()
            } else if (java.util.Arrays.equals(column.qualifier, Message)) {
              marker= column.value()
            }
          }
          executePut(
            RowKey(toProcessorId, sequenceNr).toBytes,
            Array(ProcessorId, SequenceNr, Marker, Message),
            Array(toBytes(toProcessorId), toBytes(sequenceNr), marker, msg),
            false // forceFlush to guarantee ordering
          )
          printerWriter.println(sequenceNr)
          printerWriter.flush()
          retryTimes = 0
        }
      }
      (false, "")
    }

    def handleRows(): Future[Unit] = {
      scanner.nextRows() map {
        case null =>
          scanner.close()
          Future(())
        case rows: AsyncBaseRows =>
          val (isFailed, errMsg) = writeMessages(rows)
          if (isFailed) {
            sys.error(errMsg)
            Future.failed(new Exception(errMsg))
          } else {
            handleRows()
          }
      }
    }

    initScanner
    handleRows()
  }

  private def getHBaseClient() = {
    val hBasePersistenceSettings = PluginPersistenceSettings(config, "hbase-journal")
    HBaseClientFactory.getClient(hBasePersistenceSettings, new PersistenceSettings(config.getConfig("akka.persistence")))
  }
}

