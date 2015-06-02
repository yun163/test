import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.persistence.{PersistenceSettings}
import akka.persistence.hbase.journal.{HBaseClientFactory, PluginPersistenceSettings}
import akka.persistence.hbase.common.Const._
import akka.persistence.hbase.common.Columns._
import akka.persistence.hbase.common._
import com.typesafe.config._
import java.util.{ArrayList => JArrayList}
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.KeyValue
import scala.collection.mutable
import scala.concurrent.Future
import scala.collection.JavaConverters._
import java.io._

import DeferredConversions._

object MigrateEventSource extends App {
  override def main(args: Array[String]) {
    if (args.length < 4 || !(args(3).equals("all") || args(3).equals("channel"))) {
      sys.error("Enter [formProcessorId, toProcessorId, result file, type[all total overwrite, channel marker only] to migrate")
      sys.exit(0)
    }
    val migrator = new EventSourceMigrator
    migrator.migrate(args)
  }
}

class EventSourceMigrator extends AsyncBaseUtils {
  private val config: com.typesafe.config.Config = ConfigFactory.load(getClass.getClassLoader, "migrator.conf")
  private implicit val system = ActorSystem("test", config)
  private val messagesTable = config.getString("migrator.table")
  private val messagesFamily = config.getString("migrator.family")
  private val cryptKey = config.getString("migrator.encryption-settings")
  implicit var serialization = EncryptingSerializationExtension(system, cryptKey)
  private val SCAN_MAX_NUM_ROWS = 5
  private val ReplayGapRetry = 5

  override implicit val settings = PluginPersistenceSettings(config, JOURNAL_CONFIG)
  override implicit val executionContext = system.dispatcher
  override implicit val logger: LoggingAdapter = system.log
  val channelConfirmMap:Map[String, Map[String, String]] = Map(
    "p_bw_dog" -> Map("p_bw_dog" -> "p_bw_doge"),
    "p_m_dogbtc" -> Map("p_m_dogbtc" -> "p_m_doge-btc"),
    "p_m_ltcbtc" -> Map("p_m_ltcbtc" -> "p_m_ltc-btc"),
    "p_dw" -> Map("p_bw_dog" -> "p_bw_doge")
  )
  val StartSeqNum: Int = 1
  override val client = getHBaseClient()
  var printerWriter: java.io.PrintWriter = null

  override def getTable: String = messagesTable
  override def getFamily: String = messagesFamily

  def migrate(args: Array[String]) = {
    printerWriter = new PrintWriter(new java.io.File(args(2)))
    migrateMessages(args(0), args(1), args(3), StartSeqNum, Long.MaxValue)
  }

  // "fromSeqNum" is inclusive, "toSeqNum" is exclusive
  def migrateMessages(fromProcessorId: String, toProcessorId: String, overWriteType: String, fromSeqNum: Long, toSeqNum: Long): Future[Unit] = {
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
      scanner = new SaltedScanner(serialization, client, settings.partitionCount, Bytes.toBytes(messagesTable), Bytes.toBytes(messagesFamily))
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

    def modifyMarker(original: Array[Byte]): (Boolean, Array[Byte]) = {
      val orgMk = Bytes.toString(original)
      if (channelConfirmMap.contains(fromProcessorId)) {
        val transferMap: Map[String, String] = channelConfirmMap(fromProcessorId)
        transferMap.keys foreach {
          toReplace =>
            if (orgMk.contains(toReplace)) {
              return (true, Bytes.toBytes(orgMk.replaceFirst(toReplace, transferMap(toReplace))))
            }
        }
      }
      (false, original)
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
          var writeMarker = false
          var marker: Array[Byte] = null
          for (column <- row.asScala) {
            if (java.util.Arrays.equals(column.qualifier, SequenceNr)) {
              sequenceNr = Bytes.toLong(column.value())
              tryStartSeqNr = sequenceNr + 1
            } else if (java.util.Arrays.equals(column.qualifier, Message)) {
              msg = column.value()
            } else if (java.util.Arrays.equals(column.qualifier, Marker)) {
              val res = modifyMarker(column.value())
              writeMarker = res._1
              marker = res._2
            }
          }
          println(s">>>>>>>>>>>>>>>>>>>>>>>>>>> prepare to write message [$sequenceNr, ${RowKey(toProcessorId, sequenceNr).toKeyString}, ${Bytes.toString(marker)}}]" )
          if (overWriteType.equals("all")) {
            executePut(
              RowKey(toProcessorId, sequenceNr).toBytes,
              Array(ProcessorId, SequenceNr, Marker, Message),
              Array(Bytes.toBytes(toProcessorId), Bytes.toBytes(sequenceNr), marker, msg),
              false // forceFlush to guarantee ordering
            )
          } else if (overWriteType.equals("channel")){
            if (writeMarker) {
              executePut(
                RowKey(toProcessorId, sequenceNr).toBytes,
                Array(Marker),
                Array(marker),
                false // forceFlush to guarantee ordering
              )
            }
          }
          if (writeMarker) {
            printerWriter.println(s"$sequenceNr\t${Bytes.toString(marker)}")
          } else {
            printerWriter.println(sequenceNr)
          }
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

