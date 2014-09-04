import java.util.{ArrayList => JArrayList}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.{HTableDescriptor, HColumnDescriptor}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Bytes._
import org.hbase.async.{GetRequest, PutRequest, HBaseClient, KeyValue}
import scala.concurrent.Future
import scala.collection.JavaConverters._
import com.typesafe.config._
import java.io._
import scala.concurrent.ExecutionContext.Implicits.global

import DeferredConversions._

object HBaseProber extends App {
  type AsyncBaseRow = JArrayList[KeyValue]
  override def main(args: Array[String]) {
    val config: com.typesafe.config.Config = ConfigFactory.load(getClass.getClassLoader, "prober.conf")
    val (table, family) = createTable(config, "prober.hbase", "prober.table")
    val client = new HBaseClient(config.getString("prober.hbase.zookeeper.quorum"))
    val exportPrefix: String = config.getString("prober.export.file")
    var printerWriter: java.io.PrintWriter = null
    var exportFileNow: String = ""
    val DATA: String = "d"
    client.setFlushInterval(0)
    var key: Long = 0L
    while (true) {
      if (key > 0) {
        executeGet(client, table, family, String.valueOf(key)).map {
          res =>
            res match {
              case row: AsyncBaseRow =>
                val columnValue = row.asScala.find {
                  kv => java.util.Arrays.equals(kv.qualifier(), Bytes.toBytes(DATA))
                } getOrElse {
                  throw new RuntimeException(s"Unable to find [$DATA}] field from: ${row.asScala.map(kv => Bytes.toString(kv.qualifier))}")
                }
                val lastPut = java.lang.Long.parseLong(Bytes.toString(columnValue.value()))
                if (printerWriter == null || !exportFileNow.equals(exportFile(exportPrefix))) {
                  exportFileNow = exportFile(exportPrefix)
                  if (printerWriter != null) printerWriter.close()
                  printerWriter = new PrintWriter(new java.io.File(exportFileNow))
                }
                printerWriter.println(s"$key\t${String.valueOf(lastPut)}\t${System.currentTimeMillis() - lastPut}ms")
                printerWriter.flush()
              case null =>
            }
        }
      }
      key += 1
      executePut(client, table, family, String.valueOf(key), DATA, String.valueOf(System.currentTimeMillis()))
      Thread.sleep(10 * 1000)
    }//while
  }


  def exportFile(exportPrefix: String): String = exportPrefix + new java.text.SimpleDateFormat("_YYYY_MM_dd").format(java.util.Calendar.getInstance().getTime) + ".txt"

  protected def executePut(client: HBaseClient, table: String, family: String, key: String, qualifier: String, value: String): Future[Unit] = {
    println(s">>>>>>>>>>>>>>>>>> $table, $family, $key, $qualifier, $value")
    val request = new PutRequest(table, key, family, qualifier, value)
    client.put(request).map(_ => client.flush())
  }

  protected def executeGet(client: HBaseClient, table: String, family: String, key: String): Future[AsyncBaseRow] = {
    client.get(new GetRequest(table, key).family(family))
  }

  def createTable(config: Config, hbasePath: String, probeTable: String): (String, String) = {
    val conf = getHBaseConfig(config, hbasePath)
    val admin = new HBaseAdmin(conf)
    val tableConfig = config.getConfig(probeTable)
    val table = tableConfig.getString("table")
    val familyName = tableConfig.getString("family")
    val partitionCount = tableConfig.getInt("partitionCount")
    try doInitTable(admin, table, familyName, partitionCount) finally admin.close()
    (table, familyName)
  }

  private def doInitTable(admin: HBaseAdmin, tableName: String, familyName: String, partitionCount: Int): Boolean = {
    if (admin.tableExists(tableName)) {
      val tableDesc = admin.getTableDescriptor(toBytes(tableName))
      if (tableDesc.getFamily(toBytes(familyName)) == null) {
        // target family does not exists, will add it.
        admin.addColumn(tableName, new HColumnDescriptor(familyName))
        true
      } else {
        // existing table is OK, no modifications run.
        false
      }
    } else {
      val tableDesc = new HTableDescriptor(toBytes(tableName))
      val familyDesc = genColumnFamily(toBytes(familyName), 1)
      tableDesc.addFamily(familyDesc)
      if (partitionCount > 1) {
        val splitPoints = getSplitKeys(partitionCount);
        admin.createTable(tableDesc, splitPoints)
      } else {
        admin.createTable(tableDesc)
      }
      true
    }
  }

  private def genColumnFamily(family: Array[Byte], columnMaxVersion: Int): HColumnDescriptor = {
    val familyDesc: HColumnDescriptor = new HColumnDescriptor(family)
      .setInMemory(false)
      .setMaxVersions(columnMaxVersion);
    familyDesc
  }


  private def getHBaseConfig(config: Config, hbasePath: String) = {
    val hbaseConfig = config.getConfig(hbasePath)

    @inline def hbaseKey(s: String) = "hbase." + s

    val c = new Configuration()
    hbaseConfig.entrySet().asScala foreach { e =>
      c.set(hbaseKey(e.getKey), e.getValue.unwrapped.toString)
    }
    c
  }

  private def getSplitKeys(splitNum: Int, isPrint: Boolean = false): Array[Array[Byte]] = {
    val list = collection.mutable.ListBuffer.empty[Array[Byte]]
    for (i <- 1 until splitNum) {
      val keyBytes = collection.mutable.ListBuffer.empty[Byte]
      keyBytes ++= Bytes.toBytes(padNum(i, 2))
      val zeroByte: Byte = Bytes.toBytes(0).tail(0)
      for (j <- 0 until 24) {
        keyBytes += zeroByte
      }
      val bytes = keyBytes.toArray
      if (isPrint) println(s" $i ${Bytes.toString(bytes)} ${renderBytes(bytes)}")
      list.append(bytes)
    }
    list.toArray
  }


  def padNum(l: Int, howLong: Int): String = String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString.substring(0, howLong)

  def renderBytes(bytes: Array[Byte]): String = {
    bytes.map("%02x".format(_)).mkString
  }
}

